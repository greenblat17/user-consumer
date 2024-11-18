package user_saver

import (
	"context"
	"fmt"
	"log"

	"github.com/greenblat17/platform-common/pkg/kafka"
	"github.com/greenblat17/user-consumer/internal/config"
)

type service struct {
	consumer kafka.Consumer
	config   config.UserConsumerConfig
}

// NewService return service with ser saver consumer
func NewService(consumer kafka.Consumer, config config.UserConsumerConfig) *service {
	return &service{
		consumer: consumer,
		config:   config,
	}
}

func (s *service) RunConsumer(ctx context.Context) error {
	log.Printf("kafka consumer is running")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.run(ctx):
			if err != nil {
				return err
			}
		}
	}
}

func (s *service) run(ctx context.Context) <-chan error {
	errChan := make(chan error)

	go func() {
		defer close(errChan)

		fmt.Println(s.config == nil)
		errChan <- s.consumer.Consume(ctx, s.config.TopicName(), s.SaveUserHandler)
	}()

	return errChan
}
