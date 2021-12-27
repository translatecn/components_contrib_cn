// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package metadata

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

const (
	// TTLMetadataKey 定义元数据键用于设置生存时间(以秒为单位)。
	TTLMetadataKey = "ttlInSeconds"

	// RawPayloadKey 定义了在pubsub中强迫原始有效载荷的元数据密钥。
	RawPayloadKey = "rawPayload"

	// PriorityMetadataKey 定义了用于设置优先级的元数据键。
	PriorityMetadataKey = "priority"

	// ContentType 定义了内容类型的元数据键。
	ContentType = "contentType"
)

// TryGetTTL 试着把TTL作为时间。pubsub、binding和任何其他构建块的持续时间值。
func TryGetTTL(props map[string]string) (time.Duration, bool, error) {
	if val, ok := props[TTLMetadataKey]; ok && val != "" {
		valInt64, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, false, errors.Wrapf(err, "%s 必须为有效的整数:实际 '%s'", TTLMetadataKey, val)
		}

		if valInt64 <= 0 {
			return 0, false, fmt.Errorf("%s 值必须大于零:实际为 %d", TTLMetadataKey, valInt64)
		}

		duration := time.Duration(valInt64) * time.Second
		if duration < 0 {
			// Overflow
			duration = math.MaxInt64
		}

		return duration, true, nil
	}

	return 0, false, nil
}

// TryGetPriority tries to get the priority for binding and any other building block.
func TryGetPriority(props map[string]string) (uint8, bool, error) {
	if val, ok := props[PriorityMetadataKey]; ok && val != "" {
		intVal, err := strconv.Atoi(val)
		if err != nil {
			return 0, false, errors.Wrapf(err, "%s value must be a valid integer: actual is '%s'", PriorityMetadataKey, val)
		}

		priority := uint8(intVal)
		if intVal < 0 {
			priority = 0
		} else if intVal > 255 {
			priority = math.MaxUint8
		}

		return priority, true, nil
	}

	return 0, false, nil
}

// IsRawPayload 决定是否应按原样使用有效载荷。
func IsRawPayload(props map[string]string) (bool, error) {
	if val, ok := props[RawPayloadKey]; ok && val != "" {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return false, errors.Wrapf(err, "%s Value必须是一个有效的布尔值:actual是 '%s'", RawPayloadKey, val)
		}

		return boolVal, nil
	}

	return false, nil
}

func TryGetContentType(props map[string]string) (string, bool) {
	if val, ok := props[ContentType]; ok && val != "" {
		return val, true
	}

	return "", false
}
