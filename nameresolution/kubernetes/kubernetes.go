// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"fmt"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

const (
	DefaultClusterDomain = "cluster.local"
	ClusterDomainKey     = "clusterDomain"
)

type resolver struct {
	logger        logger.Logger
	clusterDomain string
}

// NewResolver 创建Kubernetes名称解析器。
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{
		logger:        logger,
		clusterDomain: DefaultClusterDomain,
	}
}

var _ nameresolution.Resolver = &resolver{}

// Init 初始化Kubernetes名称解析器。
func (k *resolver) Init(metadata nameresolution.Metadata) error {
	configInterface, err := config.Normalize(metadata.Configuration) // 数据类型转换
	if err != nil {
		return err
	}
	if config, ok := configInterface.(map[string]string); ok {
		clusterDomain := config[ClusterDomainKey]
		if clusterDomain != "" {
			k.clusterDomain = clusterDomain
		}
	}

	return nil
}

// ResolveID 将名字解析为Kubernetes中的地址。
func (k *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	// Dapr 对于Kubernetes服务，需要这样的格式化
	return fmt.Sprintf("%s-dapr.%s.svc.%s:%d", req.ID, req.Namespace, k.clusterDomain, req.Port), nil
}
