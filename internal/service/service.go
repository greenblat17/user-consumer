package service

import "context"

// ConsumerService represents consumer service
type ConsumerService interface {
	RunConsumer(ctx context.Context) error
}
