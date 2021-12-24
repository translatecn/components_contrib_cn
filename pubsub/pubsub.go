// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import "context"

// PubSub 是消息总线的接口。
type PubSub interface {
	Init(metadata Metadata) error
	Features() []Feature
	Publish(req *PublishRequest) error
	Subscribe(req SubscribeRequest, handler Handler) error
	Close() error
}

// Handler 是用于调用应用程序处理程序的处理程序。
type Handler func(ctx context.Context, msg *NewMessage) error
