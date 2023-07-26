package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fenghaojiang/kafka-mock-producer/common/config"
	"github.com/fenghaojiang/kafka-mock-producer/common/log"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type KafkaConn struct {
	kgoClient *kgo.Client
}

func NewKafkaConn() *KafkaConn {
	brokers := viper.GetStringSlice(config.BrokerListConfigKey)
	kgoClient, err := kgo.NewClient(
		// kgo.SASL([]sasl.Mechanism{
		// 	scram.Auth{
		// 		User: "testuser",
		// 		Pass: "testuser",
		// 	}.AsSha256Mechanism(),
		// }...),
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		log.Fatal("failed to new connection", zap.Error(err))
	}

	return &KafkaConn{
		kgoClient: kgoClient,
	}
}

type Log struct {
	Type             string    `json:"type"`
	LogIndex         int       `json:"log_index"`
	TransactionHash  string    `json:"transaction_hash"`
	TransactionIndex int       `json:"transaction_index"`
	Address          string    `json:"address"`
	Data             string    `json:"data"`
	Topics           []string  `json:"topics"`
	BlockNumber      int       `json:"block_number"`
	BlockTimestamp   int       `json:"block_timestamp"`
	BlockHash        string    `json:"block_hash"`
	ItemId           string    `json:"item_id"`
	ItemTimestamp    time.Time `json:"item_timestamp"`
}

var TestLog = Log{
	Type:             "log",
	LogIndex:         275,
	TransactionHash:  "0xca814e907b31eb8619cb0fb15b9682ea89e28bfd66342a933c834d7e4f9710c2",
	TransactionIndex: 111,
	Address:          "0x551b31a8097a60d7fa4e78c4541679b79cdf8b72",
	Data:             "0x00000000000000000000000000000000000000000000000000077ca233dd9708",
	Topics: []string{
		"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
		"0x00000000000000000000000036bf7ec470420ad6bae720af98ba73a8359bbc57",
		"0x000000000000000000000000b9b404d66c206bf1a290f83d9426efc0de34b5b1",
	},
	BlockNumber:    17725560,
	BlockTimestamp: 1689749387,
	BlockHash:      "0xae29a3e338de4797150dbdfcbbe77c63cacfe9ca6ec9bbe7c5ead363b5c43507",
	ItemId:         "log_0xca814e907b31eb8619cb0fb15b9682ea89e28bfd66342a933c834d7e4f9710c2_275",
	ItemTimestamp:  time.Now(),
}

func (k *KafkaConn) Produce(ctx context.Context) error {
	topic := viper.GetString(config.TopicConfigKey)

	b, err := json.Marshal(TestLog)
	if err != nil {
		log.Error("failed to marshal message", zap.Error(err))
		return err
	}

	res := k.kgoClient.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: b,
	})

	if res.FirstErr() != nil {
		log.Error("failed to produce message", zap.Error(res.FirstErr()))
		return res.FirstErr()
	}

	log.Info("produce message successfully", zap.String("topic", topic), zap.Any("message", TestLog))

	return nil
}
