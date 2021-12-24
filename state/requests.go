// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import "github.com/dapr/components-contrib/state/query"

// GetRequest is the object describing a state fetch request.
type GetRequest struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata"`
	Options  GetStateOption    `json:"options,omitempty"`
}

// GetStateOption controls how a state store reacts to a get request.
type GetStateOption struct {
	Consistency string `json:"consistency"` // "eventual, strong"
}

// DeleteRequest 是描述一个删除状态请求的对象。
type DeleteRequest struct {
	Key      string            `json:"key"`
	ETag     *string           `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata"`
	Options  DeleteStateOption `json:"options,omitempty"`
}

// Key gets the Key on a DeleteRequest.
func (r DeleteRequest) GetKey() string {
	return r.Key
}

// Metadata gets the Metadata on a DeleteRequest.
func (r DeleteRequest) GetMetadata() map[string]string {
	return r.Metadata
}

// DeleteStateOption controls how a state store reacts to a delete request.
type DeleteStateOption struct {
	Concurrency string `json:"concurrency,omitempty"` // "concurrency"
	Consistency string `json:"consistency"`           // "eventual, strong"
}

// SetRequest 是描述一个upsert请求的对象。
type SetRequest struct {
	Key      string            `json:"key"`
	Value    interface{}       `json:"value"`
	ETag     *string           `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Options  SetStateOption    `json:"options,omitempty"`
}

// GetKey gets the Key on a SetRequest.
func (r SetRequest) GetKey() string {
	return r.Key
}

// GetMetadata gets the Key on a SetRequest.
func (r SetRequest) GetMetadata() map[string]string {
	return r.Metadata
}

// SetStateOption 控制状态存储如何对设置请求做出反应。
type SetStateOption struct {
	Concurrency string `json:"concurrency,omitempty"` // 首次写, 最后一次写
	Consistency string `json:"consistency"`           // "最终一致性、强一致性"
}

// OperationType 描述一个针对状态存储的CRUD操作。
type OperationType string

// Upsert 是一个更新或创建操作。
const Upsert OperationType = "upsert"

// Delete 是一个删除操作。
const Delete OperationType = "delete"

// TransactionalStateRequest 描述一个针对状态存储的事务性操作，包括多种类型的操作 Request字段是DeleteRequest或SetRequest。
type TransactionalStateRequest struct {
	Operations []TransactionalStateOperation `json:"operations"`
	Metadata   map[string]string             `json:"metadata,omitempty"`
}

// TransactionalStateOperation 描述了事务性操作的操作类型、键和值。
type TransactionalStateOperation struct {
	Operation OperationType `json:"operation"`
	Request   interface{}   `json:"request"`
}

// KeyInt 是一个接口，允许在请求中获取密钥和元数据。
type KeyInt interface {
	GetKey() string
	GetMetadata() map[string]string
}

type QueryRequest struct {
	Query    query.Query       `json:"query"`
	Metadata map[string]string `json:"metadata,omitempty"`
}
