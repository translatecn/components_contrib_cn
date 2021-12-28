// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	consumerID        = "consumerID"
	enableTLS         = "enableTLS"
	processingTimeout = "processingTimeout"
	redeliverInterval = "redeliverInterval"
	queueDepth        = "queueDepth"
	concurrency       = "concurrency"
	maxLenApprox      = "maxLenApprox"
)

// redisStreams 处理从Redis流中的消费，使用
// `XREADGROUP`用于读取新消息，`XPENDING`和
// `XCLAIM`用于重新发送之前失败的消息。
//
// 参见 https://redis.io/topics/streams-intro 了解更多信息
// 关于Redis流的机制。
type redisStreams struct {
	metadata       metadata
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	logger         logger.Logger

	queue chan redisMessageWrapper

	ctx    context.Context
	cancel context.CancelFunc
}

// redisMessageWrapper 封装了消息标识符、pubsub消息和处理程序，以发送到队列通道进行处理。
type redisMessageWrapper struct {
	messageID string
	message   pubsub.NewMessage
	handler   pubsub.Handler
}

// NewRedisStreams 返回一个新的redis流pub-sub实现。
func NewRedisStreams(logger logger.Logger) pubsub.PubSub {
	return &redisStreams{logger: logger}
}

func parseRedisMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{
		processingTimeout: 60 * time.Second,
		redeliverInterval: 15 * time.Second,
		queueDepth:        100,
		concurrency:       10,
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.consumerID = val
	} else {
		return m, errors.New("redis streams error: missing consumerID")
	}

	if val, ok := meta.Properties[processingTimeout]; ok && val != "" {
		if processingTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.processingTimeout = time.Duration(processingTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.processingTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: can't parse processingTimeout field: %s", err)
		}
	}

	if val, ok := meta.Properties[redeliverInterval]; ok && val != "" {
		if redeliverIntervalMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.redeliverInterval = time.Duration(redeliverIntervalMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.redeliverInterval = d
		} else {
			return m, fmt.Errorf("redis streams error: can't parse redeliverInterval field: %s", err)
		}
	}

	if val, ok := meta.Properties[queueDepth]; ok && val != "" {
		queueDepth, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse queueDepth field: %s", err)
		}
		m.queueDepth = uint(queueDepth)
	}

	if val, ok := meta.Properties[concurrency]; ok && val != "" {
		concurrency, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse concurrency field: %s", err)
		}
		m.concurrency = uint(concurrency)
	}

	if val, ok := meta.Properties[maxLenApprox]; ok && val != "" {
		maxLenApprox, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return m, fmt.Errorf("redis streams error: invalid maxLenApprox %s, %s", val, err)
		}
		m.maxLenApprox = maxLenApprox
	}

	return m, nil
}

// Init OK
func (r *redisStreams) Init(metadata pubsub.Metadata) error {
	m, err := parseRedisMetadata(metadata) // 获取到对于Redis有用的配置
	if err != nil {
		return err
	}
	r.metadata = m
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, nil)
	if err != nil {
		return err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	if _, err = r.client.Ping(r.ctx).Result(); err != nil {
		return fmt.Errorf("redis streams: 错误连接到redis at %s: %s", r.clientSettings.Host, err)
	}
	r.queue = make(chan redisMessageWrapper, int(r.metadata.queueDepth))

	for i := uint(0); i < r.metadata.concurrency; i++ {
		go r.worker()
	}

	return nil
}

func (r *redisStreams) Publish(req *pubsub.PublishRequest) error {
	streamID, err := r.client.XAdd(r.ctx, &redis.XAddArgs{
		Stream:       req.Topic,
		MaxLenApprox: r.metadata.maxLenApprox,
		Values:       map[string]interface{}{"data": req.Data},
	}).Result()
	r.logger.Infof("redis 流id%s\n", streamID)
	if err != nil {
		return fmt.Errorf("redis streams: 发布产生错误: %s", err)
	}

	return nil
}

// ------------

func (r *redisStreams) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	//XGROUP CREATE mystream mygroup $ MKSTREAM
	//XGROUP DESTROY mystream consumer-group-name
	//上面的$表示group的offset是队列中的最后一个元素，MKSTREAM这个参数会判断stream是否存在，如果不存在会创建一个我们指定名称的stream，不加这个参数，stream不存在会报错。

	//XREADGROUP GROUP mygroup Alice BLOCK 2000 COUNT 1 STREAMS mystream >
	//		这个命令是使用消费组mygroup的Alice这个消费者从mystream这个stream中读取1条消息。
	//		注意：
	//		上面使用了BLOCK，表示是阻塞读取，如果读不到数据，会阻塞等待2s，不加这个条件默认是不阻塞的
	//		">"表示只接受其他消费者没有消费过的消息
	//		如果没有">",消费者会消费比指定id偏移量大并且没有被自己确认过的消息，这样就不用关系是否ACK过或者是否BLOCK了。
	//XREAD是消费组读取消息，我们看下面这个命令：
	//XREAD COUNT 2 STREAMS mystream writers 0-0 0-0XACK mystream mygroup 1526569495631-0
	//注意：上面这个示例是从mystream和writers这2个stream中读取消息，offset都是0，COUNT参数指定了每个队列中读取的消息数量不多余2个
	err := r.client.XGroupCreateMkStream(r.ctx, req.Topic, r.metadata.consumerID, "0").Err()
	// Ignore BUSYGROUP errors
	// BUSYGROUP 消费者组名称已经存在
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		r.logger.Errorf("redis streams: %s", err)
		return err
	}

	go r.pollNewMessagesLoop(req.Topic, handler)
	go r.reclaimPendingMessagesLoop(req.Topic, handler)

	return nil
}

// enqueueMessages 是一个共享函数，它将新的消息（通过轮询）和重新交付的消息（通过回收）输送到一个通道，工人可以在那里 拾取它们进行处理。
func (r *redisStreams) enqueueMessages(stream string, handler pubsub.Handler, msgs []redis.XMessage) {
	for _, msg := range msgs {
		rmsg := createRedisMessageWrapper(stream, handler, msg)

		select {
		// Might block if the queue is full so we need the r.ctx.Done below.
		case r.queue <- rmsg:

		// Handle cancelation
		case <-r.ctx.Done():
			return
		}
	}
}

// createRedisMessageWrapper 将Redis消息、消息标识符和处理器封装在' redisMessage '中进行处理。
func createRedisMessageWrapper(stream string, handler pubsub.Handler, msg redis.XMessage) redisMessageWrapper {
	var data []byte
	if dataValue, exists := msg.Values["data"]; exists && dataValue != nil {
		switch v := dataValue.(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		}
	}

	return redisMessageWrapper{
		message: pubsub.NewMessage{
			Topic: stream,
			Data:  data,
		},
		messageID: msg.ID,
		handler:   handler,
	}
}

// worker 在独立的goroutine中运行，并从通道中提取消息进行处理。工作者的数量由`concurrency`设置来控制。
func (r *redisStreams) worker() {
	// 没有数据 会一直阻塞
	for {
		select {
		// 处理取消的问题
		case <-r.ctx.Done():
			return

		case msg := <-r.queue:
			r.processMessage(msg)
		}
	}
}

// processMessage 试图通过调用它的处理器来处理单个Redis消息。如果消息被成功处理，那么它是Ack'ed。否则，它将保持在等待列表中，并将通过' reclaimPendingMessagesLoop '重新交付。
func (r *redisStreams) processMessage(msg redisMessageWrapper) error {
	r.logger.Debugf("Processing Redis message %s", msg.messageID)
	ctx := r.ctx
	var cancel context.CancelFunc
	if r.metadata.processingTimeout != 0 && r.metadata.redeliverInterval != 0 {
		ctx, cancel = context.WithTimeout(ctx, r.metadata.processingTimeout)
		defer cancel()
	}
	if err := msg.handler(ctx, &msg.message); err != nil {
		r.logger.Errorf("Error processing Redis message %s: %v", msg.messageID, err)

		return err
	}

	if err := r.client.XAck(r.ctx, msg.message.Topic, r.metadata.consumerID, msg.messageID).Err(); err != nil {
		r.logger.Errorf("Error acknowledging Redis message %s: %v", msg.messageID, err)

		return err
	}

	return nil
}

// pollMessagesLoop calls `XReadGroup` for new messages and funnels them to the message channel
// by calling `enqueueMessages`.
func (r *redisStreams) pollNewMessagesLoop(stream string, handler pubsub.Handler) {
	for {
		// Return on cancelation
		if r.ctx.Err() != nil {
			return
		}

		// Read messages
		streams, err := r.client.XReadGroup(r.ctx, &redis.XReadGroupArgs{
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			Streams:  []string{stream, ">"},
			Count:    int64(r.metadata.queueDepth),
			Block:    time.Duration(r.clientSettings.ReadTimeout),
		}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				r.logger.Errorf("redis streams: error reading from stream %s: %s", stream, err)
			}

			continue
		}

		// Enqueue messages for the returned streams
		for _, s := range streams {
			r.enqueueMessages(s.Stream, handler, s.Messages)
		}
	}
}

// reclaimPendingMessagesLoop periodically reclaims pending messages
// based on the `redeliverInterval` setting.
func (r *redisStreams) reclaimPendingMessagesLoop(stream string, handler pubsub.Handler) {
	// Having a `processingTimeout` or `redeliverInterval` means that
	// redelivery is disabled so we just return out of the goroutine.
	if r.metadata.processingTimeout == 0 || r.metadata.redeliverInterval == 0 {
		return
	}

	// Do an initial reclaim call
	r.reclaimPendingMessages(stream, handler)

	reclaimTicker := time.NewTicker(r.metadata.redeliverInterval)

	for {
		select {
		case <-r.ctx.Done():
			return

		case <-reclaimTicker.C:
			r.reclaimPendingMessages(stream, handler)
		}
	}
}

// reclaimPendingMessages handles reclaiming messages that previously failed to process and
// funneling them to the message channel by calling `enqueueMessages`.
func (r *redisStreams) reclaimPendingMessages(stream string, handler pubsub.Handler) {
	for {
		// Retrieve pending messages for this stream and consumer
		pendingResult, err := r.client.XPendingExt(r.ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  r.metadata.consumerID,
			Start:  "-",
			End:    "+",
			Count:  int64(r.metadata.queueDepth),
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error retrieving pending Redis messages: %v", err)

			break
		}

		// Filter out messages that have not timed out yet
		msgIDs := make([]string, 0, len(pendingResult))
		for _, msg := range pendingResult {
			if msg.Idle >= r.metadata.processingTimeout {
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		// Nothing to claim
		if len(msgIDs) == 0 {
			break
		}

		// Attempt to claim the messages for the filtered IDs
		claimResult, err := r.client.XClaim(r.ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: msgIDs,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error claiming pending Redis messages: %v", err)

			break
		}

		// Enqueue claimed messages
		r.enqueueMessages(stream, handler, claimResult)

		// If the Redis nil error is returned, it means somes message in the pending
		// state no longer exist. We need to acknowledge these messages to
		// remove them from the pending list.
		if errors.Is(err, redis.Nil) {
			// Build a set of message IDs that were not returned
			// that potentially no longer exist.
			expectedMsgIDs := make(map[string]struct{}, len(msgIDs))
			for _, id := range msgIDs {
				expectedMsgIDs[id] = struct{}{}
			}
			for _, claimed := range claimResult {
				delete(expectedMsgIDs, claimed.ID)
			}

			r.removeMessagesThatNoLongerExistFromPending(stream, expectedMsgIDs, handler)
		}
	}
}

// removeMessagesThatNoLongerExistFromPending attempts to claim messages individually so that messages in the pending list
// that no longer exist can be removed from the pending list. This is done by calling `XACK`.
func (r *redisStreams) removeMessagesThatNoLongerExistFromPending(stream string, messageIDs map[string]struct{}, handler pubsub.Handler) {
	// Check each message ID individually.
	for pendingID := range messageIDs {
		claimResultSingleMsg, err := r.client.XClaim(r.ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: []string{pendingID},
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error claiming pending Redis message %s: %v", pendingID, err)

			continue
		}

		// Ack the message to remove it from the pending list.
		if errors.Is(err, redis.Nil) {
			if err = r.client.XAck(r.ctx, stream, r.metadata.consumerID, pendingID).Err(); err != nil {
				r.logger.Errorf("error acknowledging Redis message %s after failed claim for %s: %v", pendingID, stream, err)
			}
		} else {
			// This should not happen but if it does the message should be processed.
			r.enqueueMessages(stream, handler, claimResultSingleMsg)
		}
	}
}

func (r *redisStreams) Close() error {
	// context 关闭
	r.cancel()
	// 关闭客户端
	return r.client.Close()
}

func (r *redisStreams) Features() []pubsub.Feature {
	return nil
}
