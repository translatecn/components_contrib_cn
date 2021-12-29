// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

import (
	"fmt"
	"strconv"
)

// InvokeRequest dapr 输出绑定 传递的请求对象
type InvokeRequest struct {
	Data      []byte            `json:"data"`
	Metadata  map[string]string `json:"metadata"`
	Operation OperationKind     `json:"operation"`
}

// OperationKind 定义了输出绑定的操作类型
type OperationKind string

// 非详尽的操作列表。绑定可以添加不在此列表中的操作。
const (
	GetOperation    OperationKind = "get"
	CreateOperation OperationKind = "create"
	DeleteOperation OperationKind = "delete"
	ListOperation   OperationKind = "list"
)

// GetMetadataAsBool parses metadata as bool.
func (r *InvokeRequest) GetMetadataAsBool(key string) (bool, error) {
	if val, ok := r.Metadata[key]; ok {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return false, fmt.Errorf("error parsing metadata `%s` with value `%s` as bool: %w", key, val, err)
		}

		return boolVal, nil
	}

	return false, nil
}

// GetMetadataAsInt64 parses metadata as int64.
func (r *InvokeRequest) GetMetadataAsInt64(key string, bitSize int) (int64, error) {
	if val, ok := r.Metadata[key]; ok {
		intVal, err := strconv.ParseInt(val, 10, bitSize)
		if err != nil {
			return 0, fmt.Errorf("error parsing metadata `%s` with value `%s` as int%d: %w", key, val, bitSize, err)
		}

		return intVal, nil
	}

	return 0, nil
}
