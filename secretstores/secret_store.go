// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package secretstores

// SecretStore 是一个处理秘密管理的组件的接口。
type SecretStore interface {
	// Init 与实际的秘密存储进行认证，并执行其他初始操作。
	Init(metadata Metadata) error
	// GetSecret 使用密钥检索密钥，并返回解密的字符串/字符串值的映射
	GetSecret(req GetSecretRequest) (GetSecretResponse, error)
	// BulkGetSecret BulkGetSecrets 检索存储中的所有秘密，并返回解密字符串/字符串值的映射
	BulkGetSecret(req BulkGetSecretRequest) (BulkGetSecretResponse, error)
}
