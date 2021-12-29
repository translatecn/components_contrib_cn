// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

// Metadata 代表了一组绑定的具体属性。
type Metadata struct {
	Name       string // 组件的名称
	Properties map[string]string `json:"properties"`
}
