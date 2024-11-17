package app

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/greenblat17/platform-common/pkg/closer"
	"github.com/greenblat17/platform-common/pkg/kafka"
	"github.com/greenblat17/platform-common/pkg/kafka/consumer"
	"github.com/greenblat17/user-consumer/internal/config"
	"github.com/greenblat17/user-consumer/internal/config/env"
	"github.com/greenblat17/user-consumer/internal/service"
	"github.com/greenblat17/user-consumer/internal/service/consmer/user_saver"
)

type serviceProvider struct {
	userConsumerConfig config.UserConsumerConfig

	userSaverConsumer service.ConsumerService

	consumer             kafka.Consumer
	consumerGroup        sarama.ConsumerGroup
	consumerGroupHandler *consumer.GroupHandler
}

func newServiceProvider() *serviceProvider {
	return &serviceProvider{}
}

func (s *serviceProvider) UserConsumerConfig() config.UserConsumerConfig {
	if s.userConsumerConfig == nil {
		cfg, err := env.NewUserConsumerConfig()
		if err != nil {
			log.Fatalf("failed to get user consumer config: %v", err)
		}

		s.userConsumerConfig = cfg
	}

	return s.userConsumerConfig
}

func (s *serviceProvider) ConsumerGroup() sarama.ConsumerGroup {
	if s.consumerGroup == nil {
		cg, err := sarama.NewConsumerGroup(
			s.UserConsumerConfig().BrokerAddresses(),
			s.UserConsumerConfig().GroupID(),
			s.UserConsumerConfig().Config(),
		)
		if err != nil {
			log.Fatalf("failed to create consumer group: %v", err)
		}

		s.consumerGroup = cg
	}

	return s.consumerGroup
}

func (s *serviceProvider) ConsumerGroupHandler() *consumer.GroupHandler {
	if s.consumerGroupHandler == nil {
		s.consumerGroupHandler = consumer.NewGroupHandler()
	}

	return s.consumerGroupHandler
}

func (s *serviceProvider) Consumer() kafka.Consumer {
	if s.consumer == nil {
		s.consumer = consumer.NewConsumer(
			s.ConsumerGroup(),
			s.ConsumerGroupHandler(),
		)

		closer.Add(s.consumer.Close)
	}

	return s.consumer
}

func (s *serviceProvider) UserSaverConsumer() service.ConsumerService {
	if s.userSaverConsumer == nil {
		s.userSaverConsumer = user_saver.NewService(s.Consumer(), s.UserConsumerConfig())
	}

	return s.userSaverConsumer
}
