package store

import (
	"log/slog"
	"worker-pool/models"

	"gorm.io/gorm"
)

type Store struct {
	db     *gorm.DB
	logger *slog.Logger
}

func NewStore(db *gorm.DB, logger *slog.Logger) *Store {
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{db: db, logger: logger.With("component", "store")}
}

func (s *Store) GetMsgs(topic string, status models.MessageStatus) ([]models.Message, error) {
	var messages []models.Message
	q := s.db.Model(&models.Message{}).Where("status = ?", status).Limit(100)
	if topic != "" {
		q = q.Where("topic = ?", topic)
	}
	if err := q.Find(&messages).Error; err != nil {
		s.logger.Error("failed to get messages", "topic", topic, "status", status, "err", err)
		return nil, err
	}
	return messages, nil
}

func (s *Store) UpdMsgs(messageIDs []int, status models.MessageStatus) error {
	if len(messageIDs) == 0 {
		return nil
	}
	if err := s.db.Model(&models.Message{}).Where("id IN ?", messageIDs).Update("status", status).Error; err != nil {
		s.logger.Error("failed to update messages", "ids", messageIDs, "status", status, "err", err)
		return err
	}
	s.logger.Debug("updated messages", "count", len(messageIDs), "status", status)
	return nil
}

func (s *Store) CountByStatus(status models.MessageStatus) (int64, error) {
	var count int64
	if err := s.db.Model(&models.Message{}).Where("status = ?", status).Count(&count).Error; err != nil {
		s.logger.Error("failed to count messages", "status", status, "err", err)
		return 0, err
	}
	return count, nil
}

// ClaimPending atomically selects pending rows with FOR UPDATE SKIP LOCKED
// and marks them in_queue so concurrent producers cannot double-dispatch.
func (s *Store) ClaimPending(topic string, limit int) ([]models.Message, error) {
	var messages []models.Message
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Raw(`
			SELECT id, topic, message, status, created_at, updated_at
			FROM messages
			WHERE status = ? AND (? = '' OR topic = ?)
			ORDER BY id ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		`, models.MessageStatusPending, topic, topic, limit).Scan(&messages).Error; err != nil {
			return err
		}
		if len(messages) == 0 {
			return nil
		}
		ids := make([]int, len(messages))
		for i, m := range messages {
			ids[i] = m.ID
		}
		if err := tx.Model(&models.Message{}).Where("id IN ?", ids).Update("status", models.MessageStatusInQueue).Error; err != nil {
			return err
		}
		for i := range messages {
			messages[i].Status = models.MessageStatusInQueue
		}
		return nil
	})
	if err != nil {
		s.logger.Error("failed to claim pending messages", "topic", topic, "err", err)
		return nil, err
	}
	if n := len(messages); n > 0 {
		s.logger.Info("claimed messages", "count", n, "topic", topic)
	}
	return messages, nil
}
