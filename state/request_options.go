// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"fmt"
)

const (
	FirstWrite = "first-write"
	LastWrite  = "last-write"
	Strong     = "strong"
	Eventual   = "eventual"
)

// CheckRequestOptions 检查请求选项是否使用支持的关键字。
func CheckRequestOptions(options interface{}) error {
	switch o := options.(type) {
	case SetStateOption:
		if err := validateConsistencyOption(o.Consistency); err != nil {// 校验用户设置的一致性模式合不合规
			return err
		}
		if err := validateConcurrencyOption(o.Concurrency); err != nil { // 校验用户设置的数据 写入模式
			return err
		}
	case DeleteStateOption:
		// golang不支持类型转换中的多个条件，所以需要明确检查。
		if err := validateConsistencyOption(o.Consistency); err != nil {
			return err
		}
		if err := validateConcurrencyOption(o.Concurrency); err != nil {
			return err
		}
	case GetStateOption:
		if err := validateConsistencyOption(o.Consistency); err != nil {
			return err
		}
	}

	return nil
}
// 校验数据写入模式
func validateConcurrencyOption(c string) error {
	if c != "" && c != FirstWrite && c != LastWrite {
		return fmt.Errorf("不承认的并发模式 '%s'", c)
	}

	return nil
}
// 验证一致性选项
func validateConsistencyOption(c string) error {
	if c != "" && c != Strong && c != Eventual {
		return fmt.Errorf("未被认可的一致性模式 '%s'", c)
	}

	return nil
}

//下边两个函数感觉没啥用

// SetWithOptions 处理带有请求选项的SetRequest。
func SetWithOptions(method func(req *SetRequest) error, req *SetRequest) error {
	return method(req)
}

// DeleteWithOptions 处理带有选项的DeleteRequest。
func DeleteWithOptions(method func(req *DeleteRequest) error, req *DeleteRequest) error {
	return method(req)
}
