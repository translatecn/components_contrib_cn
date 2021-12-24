// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package env

import (
	"os"
	"strings"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

type envSecretStore struct {
	logger logger.Logger
}

// NewEnvSecretStore 返回一个新的env变量秘密存储。
func NewEnvSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &envSecretStore{
		logger: logger,
	}
}

// Init 创建本地秘密存储。
func (s *envSecretStore) Init(metadata secretstores.Metadata) error {
	return nil
}

// GetSecret 使用提供的键从环境变量中检索一个秘密。
func (s *envSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: os.Getenv(req.Name),
		},
	}, nil
}

// BulkGetSecret 检索存储中的所有秘密，并返回解密字符串/字符串值的映射。
func (s *envSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	r := map[string]map[string]string{}

	for _, element := range os.Environ() {
		envVariable := strings.SplitN(element, "=", 2)
		r[envVariable[0]] = map[string]string{envVariable[0]: envVariable[1]}
	}

	return secretstores.BulkGetSecretResponse{
		Data: r,
	}, nil
}

var _ secretstores.SecretStore = &envSecretStore{}
