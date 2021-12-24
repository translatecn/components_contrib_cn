// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// TransactionalStore 是一个用于初始化和支持多个事务性请求的接口。
type TransactionalStore interface {
	Init(metadata Metadata) error
	Multi(request *TransactionalStateRequest) error
}
