package producer

import (
	"context"
	"log/slog"
	"time"
	"worker-pool/consumer"
	"worker-pool/models"
	"worker-pool/store"
)

type Producer struct {
	store       *store.Store
	partition   chan chan models.Message
	maxConsumer int
	topic       string
	ctx         context.Context
	logger      *slog.Logger
	workers     []*consumer.Consumer
}

func NewProducer(store *store.Store, maxConsumer int, topic string, ctx context.Context, logger *slog.Logger) *Producer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Producer{
		store:       store,
		partition:   make(chan chan models.Message),
		maxConsumer: maxConsumer,
		topic:       topic,
		ctx:         ctx,
		logger:      logger,
	}
}

func (p *Producer) Run() error {
	log := p.logger.With("component", "producer")
	log.Info("starting workers", "count", p.maxConsumer, "topic", p.topic)
	p.workers = make([]*consumer.Consumer, 0, p.maxConsumer)
	for i := 0; i < p.maxConsumer; i++ {
		worker := consumer.NewConsumer(i+1, p.partition, p.store, p.logger)
		worker.Run()
		p.workers = append(p.workers, worker)
	}
	go p.pullMessage()
	return nil
}

func (p *Producer) Stop() {
	p.logger.With("component", "producer").Info("stopping workers", "count", len(p.workers))
	for _, w := range p.workers {
		_ = w.Stop()
	}
}

func (p *Producer) pullMessage() {
	log := p.logger.With("component", "producer")
	log.Info("pull loop started")
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			log.Info("pull loop stopped", "reason", p.ctx.Err())
			return
		case <-ticker.C:
			messages, err := p.store.ClaimPending(p.topic, 100)
			if err != nil {
				log.Error("claim pending failed", "err", err)
				continue
			}
			if len(messages) == 0 {
				continue
			}
			log.Info("dispatching batch", "count", len(messages))
			for _, message := range messages {
				select {
				case <-p.ctx.Done():
					log.Info("dispatch interrupted", "reason", p.ctx.Err())
					return
				case jobChannel := <-p.partition:
					jobChannel <- message
				}
			}
		}
	}
}
