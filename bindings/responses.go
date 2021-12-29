// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

import (
	"github.com/dapr/components-contrib/state"
)

// ReadResponse 是来自dapr输入绑定的返回对象。
type ReadResponse struct {
	Data     []byte            `json:"data"`
	Metadata map[string]string `json:"metadata"`
}

// AppResponse 是描述用户代码在绑定事件后的响应的对象。
type AppResponse struct {
	Data        interface{}        `json:"data"`
	To          []string           `json:"to"`
	StoreName   string             `json:"storeName"`
	State       []state.SetRequest `json:"state"`
	Concurrency string             `json:"concurrency"`
}

// InvokeResponse 是从输出绑定返回的响应对象。
type InvokeResponse struct {
	Data     []byte            `json:"data"`
	Metadata map[string]string `json:"metadata"`
}
