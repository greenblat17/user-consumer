package env

import (
	"errors"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

const (
	brokerAddressesEnvName = "KAFKA_BROKER_ADDRESS"
	topicNameEnvName       = "KAFKA_TOPIC"
	groupIDEnvName         = "KAFKA_GROUP_ID"
)

type userConsumerConfig struct {
	brokerAddresses []string
	groupID         string
	topicName       string
}

// NewUserConsumerConfig return config for user consumer
func NewUserConsumerConfig() (*userConsumerConfig, error) {
	brokerAddresses, ok := os.LookupEnv(brokerAddressesEnvName)
	if !ok {
		return nil, errors.New("kafka broker addresses not found")
	}

	topicName, ok := os.LookupEnv(topicNameEnvName)
	if !ok {
		return nil, errors.New("kafka topic name not found")
	}

	groupID, ok := os.LookupEnv(groupIDEnvName)
	if !ok {
		return nil, errors.New("kafka group id not found")
	}

	return &userConsumerConfig{
		brokerAddresses: strings.Split(brokerAddresses, ","),
		groupID:         groupID,
		topicName:       topicName,
	}, nil
}

func (cfg *userConsumerConfig) BrokerAddresses() []string {
	return cfg.brokerAddresses
}

func (cfg *userConsumerConfig) GroupID() string {
	return cfg.groupID
}

func (cfg *userConsumerConfig) TopicName() string {
	return cfg.topicName
}

func (cfg *userConsumerConfig) Config() *sarama.Config {
	config := sarama.NewConfig()

	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	return config
}
