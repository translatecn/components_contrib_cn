// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/kit/config"
)

type Settings struct {
	// Redis主机
	Host string `mapstructure:"redisHost"`
	// Redis的密码
	Password string `mapstructure:"redisPassword"`
	// Redis的用户名
	Username string `mapstructure:"redisUsername"`
	// 连接到服务器后要选择的数据库。
	DB int `mapstructure:"redisDB"`
	// Redis类型的节点或集群
	RedisType string `mapstructure:"redisType"`
	// 放弃前的最大重试次数。
	// 值为-1（而不是0）时，可以禁止重试。
	// 默认为3次重试
	RedisMaxRetries int `mapstructure:"redisMaxRetries"`
	// 每次重试之间的最小后退时间。默认值为8毫秒；-1禁用后退。
	RedisMinRetryInterval Duration `mapstructure:"redisMinRetryInterval"`
	// 每次重试之间的最大后退时间。默认值为512毫秒；-1禁用后退。
	RedisMaxRetryInterval Duration `mapstructure:"redisMaxRetryInterval"`
	// 建立新连接的拨号超时。
	DialTimeout Duration `mapstructure:"dialTimeout"`
	// 套接字读数的超时。如果达到了，命令将在超时后失败，而不是阻塞。 使用值-1表示没有超时，0表示默认。
	ReadTimeout Duration `mapstructure:"readTimeout"`
	// 套接字写入的超时。如果达到，命令将失败
	WriteTimeout Duration `mapstructure:"writeTimeout"`
	// 套接字连接的最大数量。
	PoolSize int `mapstructure:"poolSize"`
	// 最小的空闲连接数，这在建立新的连接很慢时很有用。
	MinIdleConns int `mapstructure:"minIdleConns"`
	// 客户端退出（关闭）连接时的连接年龄。  默认是不关闭老化连接。
	MaxConnAge Duration `mapstructure:"maxConnAge"`
	// 如果所有的连接都很忙，客户端在返回错误之前等待连接的时间。默认是ReadTimeout + 1秒。
	PoolTimeout Duration `mapstructure:"poolTimeout"`
	// 客户端关闭闲置连接的时间量。  应该小于服务器的超时时间。  默认为5分钟。-1禁止空闲超时检查。
	IdleTimeout Duration `mapstructure:"idleTimeout"`
	// 闲置连接收割机进行闲置检查的频率。  默认为1分钟。-1 禁止空闲连接reaper。 但空闲连接仍会被客户端丢弃  如果设置了IdleTimeout。
	IdleCheckFrequency Duration `mapstructure:"idleCheckFrequency"`
	// Sentinel 主体名称
	SentinelMasterName string `mapstructure:"sentinelMasterName"`
	// 使用Redis Sentinel进行自动故障转移.
	Failover bool `mapstructure:"failover"`

	// 一个标志，通过设置InsecureSkipVerify为 "true "来启用TLS。
	EnableTLS bool `mapstructure:"enableTLS"`
}

func (s *Settings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	return nil
}

type Duration time.Duration

func (r *Duration) DecodeString(value string) error {
	if val, err := strconv.Atoi(value); err == nil {
		if val < 0 {
			*r = Duration(val)

			return nil
		}
		*r = Duration(time.Duration(val) * time.Millisecond)

		return nil
	}

	// Convert it by parsing
	d, err := time.ParseDuration(value)
	if err != nil {
		return err
	}

	*r = Duration(d)

	return nil
}
