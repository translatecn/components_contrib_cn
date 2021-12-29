// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

// InputBinding 是定义一个在传入事件上触发的绑定的接口。
type InputBinding interface {
	// Init 将连接和属性元数据传递给绑定实现
	Init(metadata Metadata) error
	// Read 是一个阻塞方法，每当有事件发生时就会触发回调函数。
	Read(handler func(*ReadResponse) ([]byte, error)) error
}
