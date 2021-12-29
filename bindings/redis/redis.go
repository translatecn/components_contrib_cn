// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"

	"github.com/dapr/components-contrib/bindings"
	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/kit/logger"
)

// Redis is a redis output binding.
type Redis struct {
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	logger         logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRedis 返回Redis绑定
func NewRedis(logger logger.Logger) *Redis {
	return &Redis{logger: logger}
}

// Init 执行元数据解析和连接创建。
func (r *Redis) Init(meta bindings.Metadata) (err error) {
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(meta.Properties, nil)
	if err != nil {
		return err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	_, err = r.client.Ping(r.ctx).Result()
	if err != nil {
		return fmt.Errorf("redis binding:连接redis失败 %s: %s", r.clientSettings.Host, err)
	}

	return err
}

// Operations 返回支持的操作类型
func (r *Redis) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (r *Redis) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if val, ok := req.Metadata["key"]; ok && val != "" {
		key := val
		_, err := r.client.Do(r.ctx, "SET", key, req.Data).Result()
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	return nil, errors.New("redis binding: 写入请求元数据的键丢失")
}

func (r *Redis) Close() error {
	r.cancel()

	return r.client.Close()
}

//var _ bindings.InputBinding= &Redis{} 没有实现
var _ bindings.OutputBinding = &Redis{}
