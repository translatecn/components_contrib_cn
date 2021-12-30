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
		return m, errors.New("redis流错误：缺少consumerID")
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
	// 在pub组件初始化时，就声明了
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
	//上面的$表示group的offset是队列中的最后一个元素，MKSTREAM这个参数会判断stream是否存在，如果不存在会创建一个我们指定名称的stream，不加这个参数，stream不存在会报错。
	//XGROUP DESTROY mystream consumer-group-name

	//XREADGROUP GROUP mygroup Alice BLOCK 2000 COUNT 1 STREAMS mystream >
	//		这个命令是使用消费组mygroup的Alice这个消费者从mystream这个stream中读取1条消息。
	//		注意：
	//		上面使用了BLOCK，表示是阻塞读取，如果读不到数据，会阻塞等待2s，不加这个条件默认是不阻塞的
	//		">"表示只接受其他消费者没有消费过的消息
	//		如果没有">",消费者会消费比指定id偏移量大并且没有被自己确认过的消息，这样就不用关系是否ACK过或者是否BLOCK了。
	//XREAD是消费组读取消息，我们看下面这个命令：
	//XREAD COUNT 2 STREAMS mystream writers 0-0 0-0XACK mystream mygroup 1526569495631-0
	//注意：上面这个示例是从mystream和writers这2个stream中读取消息，offset都是0，COUNT参数指定了每个队列中读取的消息数量不多余2个

	// consumerID 为pub声明的、或者是本应用的ID
	// 如果执行到此；分为两种
	// 1：自身应用
	// 2：指定了消费者组
	err := r.client.XGroupCreateMkStream(r.ctx, req.Topic, r.metadata.consumerID, "0").Err()
	// Ignore BUSYGROUP errors
	// BUSYGROUP 消费者组名称已经存在
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		r.logger.Errorf("redis streams: %s", err)
		return err
	}

	go r.pollNewMessagesLoop(req.Topic, handler)        // 轮询新消息的循环
	go r.reclaimPendingMessagesLoop(req.Topic, handler) // 回收待处理的信息循环

	return nil
}

// enqueueMessages 是一个共享函数，它将新的消息（通过轮询）和重新交付的消息（通过回收）输送到一个通道，工人可以在那里 拾取它们进行处理。
func (r *redisStreams) enqueueMessages(stream string, handler pubsub.Handler, msgs []redis.XMessage) {
	for _, msg := range msgs {
		rmsg := createRedisMessageWrapper(stream, handler, msg)

		select {
		// 如果队列已满，可能会阻塞，所以我们需要下面的r.ctx.Done。
		case r.queue <- rmsg:

		// 处理取消信号
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
			Topic: stream, // 流的名称
			Data:  data,   // 消息主体
		},
		messageID: msg.ID,  // 消息ID
		handler:   handler, // 处理方法
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

		case msg := <-r.queue: // 只有订阅了才会往这个队列里塞数据
			r.processMessage(msg) // 处理消息
		}
	}
}

// processMessage 试图通过调用它的处理器来处理单个Redis消息。如果消息被成功处理，那么它是Ack'ed。否则，它将保持在等待列表中，并将通过' reclaimPendingMessagesLoop '重新交付。
func (r *redisStreams) processMessage(msg redisMessageWrapper) error {
	r.logger.Debugf("处理Redis消息 %s", msg.messageID)
	ctx := r.ctx
	var cancel context.CancelFunc
	if r.metadata.processingTimeout != 0 && r.metadata.redeliverInterval != 0 {
		ctx, cancel = context.WithTimeout(ctx, r.metadata.processingTimeout)
		defer cancel()
	}
	// dapr 封装的接收到订阅消息与APP交互的逻辑
	if err := msg.handler(ctx, &msg.message); err != nil {
		r.logger.Errorf("处理Redis信息出错 %s: %v", msg.messageID, err)
		return err
	}

	if err := r.client.XAck(r.ctx, msg.message.Topic, r.metadata.consumerID, msg.messageID).Err(); err != nil {
		r.logger.Errorf("确认Redis消息的出错 %s: %v", msg.messageID, err)
		return err
	}

	return nil
}

// pollMessagesLoop calls `XReadGroup` 来获取新的消息，并通过调用 "enqueueMessages "将其输送到消息通道。
func (r *redisStreams) pollNewMessagesLoop(stream string, handler pubsub.Handler) {
	for {
		// Return on cancelation
		if r.ctx.Err() != nil {
			return
		}

		//XREADGROUP GROUP mygroup Alice BLOCK 2000 COUNT 1 STREAMS mystream >
		//		这个命令是使用消费组mygroup的Alice这个消费者从mystream这个stream中读取1条消息。
		//		注意：
		//		上面使用了BLOCK，表示是阻塞读取，如果读不到数据，会阻塞等待2s，不加这个条件默认是不阻塞的
		//		">"表示只接受其他消费者没有消费过的消息
		//		如果没有">",消费者会消费比指定id偏移量大并且没有被自己确认过的消息，这样就不用关系是否ACK过或者是否BLOCK了。
		streams, err := r.client.XReadGroup(r.ctx, &redis.XReadGroupArgs{
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			Streams:  []string{stream, ">"},
			Count:    int64(r.metadata.queueDepth),
			Block:    time.Duration(r.clientSettings.ReadTimeout), // 一定要设置，不然死循环 CPU占用很多, 为0 的话，就一直阻塞
		}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				r.logger.Errorf("redis流：从流中读取错误 %s: %s", stream, err)
			}

			continue
		}

		// 对返回的流进行信息排队
		for _, s := range streams {
			r.enqueueMessages(s.Stream, handler, s.Messages)
		}
	}
}

// reclaimPendingMessagesLoop 根据 "redeliverInterval "的设置，定期回收待发信息。     【重新交付的时间间隔】
func (r *redisStreams) reclaimPendingMessagesLoop(stream string, handler pubsub.Handler) {
	// 有`processingTimeout`或`redeliverInterval`意味着重新交付被禁用，所以我们只是从goroutine中返回。
	if r.metadata.processingTimeout == 0 || r.metadata.redeliverInterval == 0 {
		return
	}

	// 做一个初步的回收调用
	r.reclaimPendingMessages(stream, handler)
	// 重试单位是毫秒
	reclaimTicker := time.NewTicker(r.metadata.redeliverInterval) // 重试时间

	for {
		select {
		case <-r.ctx.Done():
			return

		case <-reclaimTicker.C:
			r.reclaimPendingMessages(stream, handler)
		}
	}
}

// reclaimPendingMessages 通过调用`enqueueMessages`，处理回收先前未能处理的消息，并将其输送到消息通道。
func (r *redisStreams) reclaimPendingMessages(stream string, handler pubsub.Handler) {
	for {
		//检索此流和customer的待处理消息 : 查询group 未确认的消息；而不是具体某个customer的消息
		pendingResult, err := r.client.XPendingExt(r.ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  r.metadata.consumerID,
			Start:  "-",
			End:    "+",
			Count:  int64(r.metadata.queueDepth),
		}).Result() // 获取没有确认的消息列表，最大3 个
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("检索待处理的Redis信息时出错: %v", err)
			break
		}

		// 过滤掉还没有超时的信息
		msgIDs := make([]string, 0, len(pendingResult))
		for _, msg := range pendingResult {
			if msg.Idle >= r.metadata.processingTimeout {
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		// 没有什么可要求的
		if len(msgIDs) == 0 {
			break
		}

		// 使用 XCLAIM 来获得消息的所有权，并使其重新分配
		claimResult, err := r.client.XClaim(r.ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: msgIDs,
		}).Result()
		//消息只有在其空闲时间大于我们通过 XCLAIM 指定的空闲时间的时才会被认领。
		//因为作为一个副作用，XCLAIM 也会重置消息的空闲时间（因为这是处理消息的一次新尝试），
		//两个试图同时认领消息的消费者将永远不会成功：只有一个消费者能成功认领消息。
		//这避免了我们用微不足道的方式多次处理给定的消息（虽然一般情况下无法完全避免多次处理）。
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("错误要求待处理的Redis消息: %v", err)
			break
		}

		// 这里会阻塞直至能塞入到队列里
		r.enqueueMessages(stream, handler, claimResult)
		//如果返回Redis nil错误，这意味着某些处于待处理状态的消息不再存在。
		if errors.Is(err, redis.Nil) {
			// 建立一套没有返回的、可能不再存在的消息ID。
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

// removeMessagesThatNoLongerExistFromPending `XACK`.
// 试图单独要求信息，以便将待定列表中不再存在的信息从待定列表中删除。这可以通过调用
// removeMessagesThatNoLongerExistFromPending attempts to claim messages individually so that messages in the pending list
// that no longer exist can be removed from the pending list. This is done by calling `XACK`.
func (r *redisStreams) removeMessagesThatNoLongerExistFromPending(stream string, messageIDs map[string]struct{}, handler pubsub.Handler) {
	// 单独检查每个信息ID。
	for pendingID := range messageIDs {
		claimResultSingleMsg, err := r.client.XClaim(r.ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: []string{pendingID},
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("错误声称待处理的Redis信息 %s: %v", pendingID, err)

			continue
		}

		// 确认该信息，将其从待处理列表中删除。
		if errors.Is(err, redis.Nil) {
			if err = r.client.XAck(r.ctx, stream, r.metadata.consumerID, pendingID).Err(); err != nil {
				r.logger.Errorf("error acknowledging Redis message %s after failed claim for %s: %v", pendingID, stream, err)
			}
		} else {
			// 这种情况不应该发生，但如果发生了，信息应该被处理。
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
