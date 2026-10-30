package models

import "time"

type Message struct {
	ID        int           `json:"id" gorm:"primaryKey"`
	Topic     string        `json:"topic" gorm:"index"`
	Message   string        `json:"message"`
	Status    MessageStatus `json:"status" gorm:"index"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

func (Message) TableName() string {
	return "messages"
}
