// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

const (
	// FeatureMessageTTL 是处理消息TTL的特性。
	FeatureMessageTTL Feature = "MESSAGE_TTL"
)

// Feature 命名一个可以由PubSub组件实现的特性。
type Feature string

// IsPresent 检查给定的特性是否在列表中。
func (f Feature) IsPresent(features []Feature) bool {
	for _, feature := range features {
		if feature == f {
			return true
		}
	}

	return false
}
