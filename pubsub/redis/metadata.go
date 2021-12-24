// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"time"
)

type metadata struct {
	// 消费者标识符
	consumerID string
	// 检查未决信息到重新交付的间隔时间（0禁止重新交付）。
	redeliverInterval time.Duration
	// 在试图重新交付信息之前，信息必须处于等待状态的时间（0禁止重新交付）。
	processingTimeout time.Duration
	// 用于处理的消息队列的大小
	queueDepth uint
	// 正在处理消息的并发工作者的数量
	concurrency uint

	// 流的最大长度
	maxLenApprox int64
}
