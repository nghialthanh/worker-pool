package consumer

import (
	"log/slog"
	"time"
	"worker-pool/models"
	"worker-pool/store"
)

type Consumer struct {
	id         int
	workerPool chan chan models.Message
	jobChannel chan models.Message
	quit       chan struct{}
	store      *store.Store
	logger     *slog.Logger
}

func NewConsumer(id int, partition chan chan models.Message, store *store.Store, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		id:         id,
		workerPool: partition,
		jobChannel: make(chan models.Message),
		quit:       make(chan struct{}),
		store:      store,
		logger:     logger.With("component", "consumer", "worker_id", id),
	}
}

func (c *Consumer) Run() {
	go func() {
		c.logger.Info("worker started")
		for {
			select {
			case c.workerPool <- c.jobChannel:
				select {
				case msg := <-c.jobChannel:
					start := time.Now()
					if err := c.process(msg); err != nil {
						c.logger.Error("failed to process message", "message_id", msg.ID, "err", err)
						continue
					}
					c.logger.Info("processed message",
						"message_id", msg.ID,
						"topic", msg.Topic,
						"duration", time.Since(start),
					)
				case <-c.quit:
					c.logger.Info("worker stopped")
					return
				}
			case <-c.quit:
				c.logger.Info("worker stopped")
				return
			}
		}
	}()
}

func (c *Consumer) Stop() error {
	close(c.quit)
	return nil
}

func (c *Consumer) process(message models.Message) error {
	// Simulate work under load.
	time.Sleep(50 * time.Millisecond)
	if err := c.store.UpdMsgs([]int{message.ID}, models.MessageStatusSuccess); err != nil {
		_ = c.store.UpdMsgs([]int{message.ID}, models.MessageStatusFailed)
		return err
	}
	return nil
}
