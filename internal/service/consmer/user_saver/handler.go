package user_saver

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	"github.com/greenblat17/user-consumer/internal/model"
)

func (s *service) SaveUserHandler(_ context.Context, msg *sarama.ConsumerMessage) error {
	userInfo := &model.UserInfo{}

	err := json.Unmarshal(msg.Value, userInfo)
	if err != nil {
		return err
	}

	log.Printf("user info consumed from kafka: %#v", userInfo)

	return nil
}
