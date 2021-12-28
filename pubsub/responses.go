// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

// AppResponseStatus 代表一个PubSub响应的状态.
type AppResponseStatus string

const (
	// Success 意味着该信息被正确接收和处理。
	Success AppResponseStatus = "SUCCESS"
	// Retry 意味着该消息已收到，但无法处理，必须重试。
	Retry AppResponseStatus = "RETRY"
	// Drop 意味着该信息已收到，但不应被处理。
	Drop AppResponseStatus = "DROP"
)

// AppResponse 是描述pubsub事件后用户代码的响应的对象。
type AppResponse struct {
	Status AppResponseStatus `json:"status"`
}
