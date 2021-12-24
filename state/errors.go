// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"errors"
	"fmt"
)

type ETagErrorKind string

const (
	mismatchPrefix = "可能的etag不匹配。来自状态存储的错误"
	invalidPrefix  = "无效的ETAG值"

	ETagInvalid  ETagErrorKind = "无效"
	ETagMismatch ETagErrorKind = "不匹配"
)

// ETagError is a custom error type for etag exceptions.
type ETagError struct {
	err  error
	kind ETagErrorKind
}

func (e *ETagError) Kind() ETagErrorKind {
	return e.kind
}

func (e *ETagError) Error() string {
	var prefix string

	switch e.kind {
	case ETagInvalid:
		prefix = invalidPrefix
	case ETagMismatch:
		prefix = mismatchPrefix
	}

	if e.err != nil {
		return fmt.Sprintf("%s: %s", prefix, e.err)
	}

	return errors.New(prefix).Error()
}

// NewETagError 返回一个包裹着现有上下文错误的ETagError。
func NewETagError(kind ETagErrorKind, err error) *ETagError {
	return &ETagError{
		err:  err,
		kind: kind,
	}
}
