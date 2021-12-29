// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

// OutputBinding 是一个输出绑定的接口，允许用户用可选的有效载荷来调用远程系统。
type OutputBinding interface {
	Init(metadata Metadata) error
	Invoke(req *InvokeRequest) (*InvokeResponse, error)
	Operations() []OperationKind
}
