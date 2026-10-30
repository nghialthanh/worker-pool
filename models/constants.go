package models

type MessageStatus int

const (
	MessageStatusPending MessageStatus = iota
	MessageStatusInQueue
	MessageStatusSuccess
	MessageStatusFailed
)
