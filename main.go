package main

import (
	"context"
	"fmt"

	"time"

	"github.com/fenghaojiang/kafka-mock-producer/common/config"
	"github.com/fenghaojiang/kafka-mock-producer/common/log"
	"github.com/fenghaojiang/kafka-mock-producer/kafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	var command = &cobra.Command{
		Use: "kafka-mock-producer",
		RunE: func(cmd *cobra.Command, args []string) error {
			kafkaConn := kafka.NewKafkaConn()

			for {
				select {
				case <-time.After(time.Duration(viper.GetInt(config.IntervalConfigKey)) * time.Second):
					if err := kafkaConn.Produce(cmd.Context()); err != nil {
						log.Error("failed to produce message", zap.Error(err))
					}
				case <-cmd.Context().Done():
					return nil
				}
			}

			return nil
		},
	}

	command.PersistentFlags().StringArray(config.BrokerListConfigKey, []string{"localhost:9092"}, "The Kafka broker list")
	command.PersistentFlags().String(config.TopicConfigKey, "test", "The Kafka topic")
	command.PersistentFlags().Int(config.IntervalConfigKey, 1, "The interval in seconds between each message")
	command.PersistentFlags().String(config.MessageConfigKey, "message", "The message to send")

	command.PreRunE = func(cmd *cobra.Command, args []string) error {
		if err := viper.BindPFlags(cmd.Flags()); err != nil {
			return fmt.Errorf("bind flags: %w", err)
		}
		return nil
	}

	if err := command.ExecuteContext(context.Background()); err != nil {
		log.Fatal("failed to executing command: ", zap.Error(err))
	}
}
