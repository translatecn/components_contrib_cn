// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	key = "partitionKey"
)

// Kafka 允许读取/写入Kafka消费者组。
type Kafka struct {
	producer      sarama.SyncProducer
	topics        []string // 主题s
	consumerGroup string   // 消费者组
	brokers       []string // 节点
	publishTopic  string   // 发布主题
	authRequired  bool     // 是否需要认证
	saslUsername  string   // 用户名
	saslPassword  string   // 密码
	initialOffset int64    // 初始偏移量
	logger        logger.Logger
}

type kafkaMetadata struct {
	Brokers         []string `json:"brokers"`
	Topics          []string `json:"topics"`
	PublishTopic    string   `json:"publishTopic"`
	ConsumerGroup   string   `json:"consumerGroup"`
	AuthRequired    bool     `json:"authRequired"`
	SaslUsername    string   `json:"saslUsername"`
	SaslPassword    string   `json:"saslPassword"`
	InitialOffset   int64    `json:"initialOffset"`
	MaxMessageBytes int
}

type consumer struct {
	ready    chan bool
	callback func(*bindings.ReadResponse) ([]byte, error)
}

// ConsumeClaim 消费消息
func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// ./kafka-console-producer.sh --broker-list localhost:9092 --topic topic-a
		if consumer.callback != nil {
			_, err := consumer.callback(&bindings.ReadResponse{
				Data: message.Value,
			})
			if err == nil {
				//标记一个信息为已消耗。
				session.MarkMessage(message, "")
			}
		}
	}

	return nil
}

// Setup 当消费完一条数据，执行此函数
func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// NewKafka 返回kafka binding实例
func NewKafka(logger logger.Logger) *Kafka {
	return &Kafka{logger: logger}
}

var _ bindings.InputBinding = &Kafka{}

// Init 元数据解析、建立链接
func (k *Kafka) Init(metadata bindings.Metadata) error {
	meta, err := k.getKafkaMetadata(metadata)
	if err != nil {
		return err
	}

	p, err := k.getSyncProducer(meta) // 获取同步发送者
	if err != nil {
		return err
	}

	k.brokers = meta.Brokers
	k.producer = p
	k.topics = meta.Topics
	k.publishTopic = meta.PublishTopic
	k.consumerGroup = meta.ConsumerGroup
	k.authRequired = meta.AuthRequired
	k.initialOffset = meta.InitialOffset

	if meta.AuthRequired {
		k.saslUsername = meta.SaslUsername
		k.saslPassword = meta.SaslPassword
	}

	return nil
}

// GetKafkaMetadata 返回kafka元数据
func (k *Kafka) getKafkaMetadata(metadata bindings.Metadata) (*kafkaMetadata, error) {
	meta := kafkaMetadata{}
	meta.ConsumerGroup = metadata.Properties["consumerGroup"]
	meta.PublishTopic = metadata.Properties["publishTopic"]

	initialOffset, err := parseInitialOffset(metadata.Properties["initialOffset"])
	if err != nil {
		return nil, err
	}
	meta.InitialOffset = initialOffset

	if val, ok := metadata.Properties["brokers"]; ok && val != "" {
		meta.Brokers = strings.Split(val, ",")
	}
	if val, ok := metadata.Properties["topics"]; ok && val != "" {
		meta.Topics = strings.Split(val, ",")
	}

	val, ok := metadata.Properties["authRequired"]
	if !ok {
		return nil, errors.New("kafka error: 丢失authRequired属性")
	}
	if val == "" {
		return nil, errors.New("kafka error: 'authRequired'属性为空")
	}
	validAuthRequired, err := strconv.ParseBool(val)
	if err != nil {
		return nil, errors.New("kafka error: 'authRequired' 值无效")
	}
	meta.AuthRequired = validAuthRequired

	// 如果 authRequired 为FALSE 忽略 SASL 配置
	if meta.AuthRequired {
		if val, ok := metadata.Properties["saslUsername"]; ok && val != "" {
			meta.SaslUsername = val
		} else {
			return nil, errors.New("kafka error: 丢失 SASL Username")
		}

		if val, ok := metadata.Properties["saslPassword"]; ok && val != "" {
			meta.SaslPassword = val
		} else {
			return nil, errors.New("kafka error: 丢失 SASL Password")
		}
	}

	if val, ok := metadata.Properties["maxMessageBytes"]; ok && val != "" {
		maxBytes, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("kafka error: 不能解析 maxMessageBytes: %s", err)
		}

		meta.MaxMessageBytes = maxBytes
	}

	return &meta, nil
}

// Read   pkg/runtime/binding.go:285
func (k *Kafka) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Offsets.Initial = k.initialOffset
	//
	if k.authRequired {
		updateAuthInfo(config, k.saslUsername, k.saslPassword)
	}
	c := consumer{
		callback: handler,
		ready:    make(chan bool),
	}

	client, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err = client.Consume(ctx, k.topics, &c); err != nil {
				k.logger.Errorf("error from c: %s", err)
			}
			// 检查上下文是否被取消，标志着C应该停止。
			if ctx.Err() != nil {
				return
			}
			c.ready = make(chan bool)
		}
	}()

	<-c.ready

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		return err
	}

	return nil
}

var _ bindings.OutputBinding = &Kafka{}

func (k *Kafka) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (k *Kafka) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	msg := &sarama.ProducerMessage{
		Topic: k.publishTopic,
		Value: sarama.ByteEncoder(req.Data),
	}
	if val, ok := req.Metadata[key]; ok && val != "" {
		msg.Key = sarama.StringEncoder(val)
	}

	_, _, err := k.producer.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// 获取同步生产者
func (k *Kafka) getSyncProducer(meta *kafkaMetadata) (sarama.SyncProducer, error) {
	config := sarama.NewConfig() // 返回一个具有合理默认值的新配置实例。
	// NoResponse   不发送任何响应，TCP ACK就是你得到的全部。
	// WaitForLocal 等到只有本地提交成功后再进行响应。
	// WaitForAll   等待所有同步复制体提交后再进行响应。同步复制的最小数量是通过以下方式在代理上配置的 min.insync.replicas "配置键配置。
	config.Producer.RequiredAcks = sarama.WaitForAll //
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Version = sarama.V1_0_0_0

	// 如果authRequired is false  ignore SASL properties
	if meta.AuthRequired {
		updateAuthInfo(config, meta.SaslUsername, meta.SaslPassword)
	}

	if meta.MaxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = meta.MaxMessageBytes
	}

	producer, err := sarama.NewSyncProducer(meta.Brokers, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// 更新认证配置
func updateAuthInfo(config *sarama.Config, saslUsername, saslPassword string) {
	config.Net.SASL.Enable = true
	config.Net.SASL.User = saslUsername
	config.Net.SASL.Password = saslPassword
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext // 代表SASL/PLAIN机制

	config.Net.TLS.Enable = true
	// nolint: gosec
	config.Net.TLS.Config = &tls.Config{
		// InsecureSkipVerify: true,
		ClientAuth: 0,
	}
}

func (k *Kafka) Close() error {
	if err := k.producer.Close(); err != nil {
		k.logger.Errorf("kafka error:关闭生产者失败: %v", err)

		return err
	}

	return nil
}

//解析初始偏移量
func parseInitialOffset(value string) (initialOffset int64, err error) {
	initialOffset = sarama.OffsetNewest // Default
	if strings.EqualFold(value, "oldest") {
		initialOffset = sarama.OffsetOldest
	} else if strings.EqualFold(value, "newest") {
		initialOffset = sarama.OffsetNewest
	} else if value != "" {
		return 0, fmt.Errorf("kafka error: 无效的偏移量: %s", value)
	}

	return initialOffset, err
}

// 如果保证消息不丢失
// 1、producer.type=sync           	client
// 2、client ack waitall		  	client
// 3、replication.factor>=2		  	broker
// 4、min.insync.replicas>=2		broker
