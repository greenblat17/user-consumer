package config

import (
	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

// UserConsumerConfig is the configuration for user consumer
type UserConsumerConfig interface {
	BrokerAddresses() []string
	TopicName() string
	GroupID() string
	Config() *sarama.Config
}

// Load loads configuration from environment variables file
func Load(path string) error {
	err := godotenv.Load(path)
	if err != nil {
		return err
	}

	return nil
}
