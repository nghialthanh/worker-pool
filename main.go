package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"worker-pool/models"
	"worker-pool/producer"
	"worker-pool/store"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(envOr("LOG_LEVEL", "INFO")),
	}))
	slog.SetDefault(logger)

	dsn := envOr("DATABASE_URL", "host=localhost user=app password=app dbname=workerpool port=5433 sslmode=disable TimeZone=UTC")
	workers := envInt("MAX_WORKERS", 8)
	topic := envOr("TOPIC", "stress")

	logger.Info("starting worker pool", "workers", workers, "topic", topic)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Warn),
	})
	if err != nil {
		logger.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}

	sqlDB, err := db.DB()
	if err != nil {
		logger.Error("failed to get sql db", "err", err)
		os.Exit(1)
	}
	sqlDB.SetMaxOpenConns(workers * 2)
	sqlDB.SetMaxIdleConns(workers)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := db.AutoMigrate(&models.Message{}); err != nil {
		logger.Error("auto-migrate failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	s := store.NewStore(db, logger)
	p := producer.NewProducer(s, workers, topic, ctx, logger)
	if err := p.Run(); err != nil {
		logger.Error("producer failed to start", "err", err)
		os.Exit(1)
	}

	go reportProgress(ctx, s, logger)

	<-ctx.Done()
	logger.Info("shutdown signal received")
	p.Stop()
	_ = sqlDB.Close()
	logger.Info("bye")
}

func reportProgress(ctx context.Context, s *store.Store, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, _ := s.CountByStatus(models.MessageStatusPending)
			queued, _ := s.CountByStatus(models.MessageStatusInQueue)
			success, _ := s.CountByStatus(models.MessageStatusSuccess)
			failed, _ := s.CountByStatus(models.MessageStatusFailed)
			logger.Info("status snapshot",
				"pending", pending,
				"in_queue", queued,
				"success", success,
				"failed", failed,
			)
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
