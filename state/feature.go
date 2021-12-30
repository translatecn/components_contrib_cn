// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

const (
	// FeatureETag 是在状态存储中对元数据进行标记的功能。
	FeatureETag Feature = "ETAG"
	// FeatureTransactional 是执行事务性操作的功能。
	FeatureTransactional Feature = "TRANSACTIONAL"
)

// Feature 命名一个可以由PubSub组件实现的功能。
type Feature string

// IsPresent 检查一个给定的特征是否在列表中存在。
func (f Feature) IsPresent(features []Feature) bool {
	for _, feature := range features {
		if feature == f {
			return true
		}
	}

	return false
}
