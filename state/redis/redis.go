// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/agrea/ptr"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

const (
	setQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if type(var1) == \"table\" then redis.call(\"DEL\", KEYS[1]); end; local var2 = redis.pcall(\"HGET\", KEYS[1], \"first-write\"); if not var1 or type(var1)==\"table\" or var1 == \"\" or var1 == ARGV[1] or (not var2 and ARGV[1] == \"0\") then redis.call(\"HSET\", KEYS[1], \"data\", ARGV[2]); if ARGV[3] == \"0\" then redis.call(\"HSET\", KEYS[1], \"first-write\", 0); end; return redis.call(\"HINCRBY\", KEYS[1], \"version\", 1) else return error(\"failed to set key \" .. KEYS[1]) end"
	delQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if not var1 or type(var1)==\"table\" or var1 == ARGV[1] or var1 == \"\" or ARGV[1] == \"0\" then return redis.call(\"DEL\", KEYS[1]) else return error(\"failed to delete \" .. KEYS[1]) end"
	connectedSlavesReplicas  = "connected_slaves:"
	infoReplicationDelimiter = "\r\n"
	maxRetries               = "maxRetries"
	maxRetryBackoff          = "maxRetryBackoff"
	ttlInSeconds             = "ttlInSeconds"
	defaultBase              = 10
	defaultBitSize           = 0
	defaultDB                = 0
	defaultMaxRetries        = 3
	defaultMaxRetryBackoff   = time.Second * 2
)

// StateStore 是一个Redis状态存储。
type StateStore struct {
	state.DefaultBulkStore
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	json           jsoniter.API
	metadata       metadata
	replicas       int // 从机副本数

	features []state.Feature
	logger   logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRedisStateStore 返回一个新的redis状态存储。
func NewRedisStateStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// 从redis state 中解析出一些元数据
func parseRedisMetadata(meta state.Metadata) (metadata, error) {
	m := metadata{}
	//     - name: redisHost
	//      value: "dapr-redis-svc:45454"
	//    - name: redisPassword
	//      value: "1234"
	//    - name: actorStateStore
	//      value: true
	//    - name: maxRetries
	//      value: "1"
	m.maxRetries = defaultMaxRetries // 重试次数
	if val, ok := meta.Properties[maxRetries]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse maxRetries field: %s", err)
		}
		m.maxRetries = int(parsedVal)
	}
	//默认的最大重试后退时间
	m.maxRetryBackoff = defaultMaxRetryBackoff
	if val, ok := meta.Properties[maxRetryBackoff]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: 无法解析maxRetryBackoff字段: %s", err)
		}
		m.maxRetryBackoff = time.Duration(parsedVal)
	}

	if val, ok := meta.Properties[ttlInSeconds]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: 无法解析ttlInSeconds字段: %s", err)
		}
		intVal := int(parsedVal)
		m.ttlInSeconds = &intVal
	} else {
		m.ttlInSeconds = nil
	}

	return m, nil
}

func (r *StateStore) Ping() error {
	if _, err := r.client.Ping(context.Background()).Result(); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return nil
}

// Init 做元数据和连接解析。
func (r *StateStore) Init(metadata state.Metadata) error {
	m, err := parseRedisMetadata(metadata) // 从redis state 中解析出一些元数据
	if err != nil {
		return err
	}
	r.metadata = m

	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.maxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.maxRetryBackoff)}
	// 已经与Redis建立了连接
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, &defaultSettings)
	if err != nil {
		return err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	if _, err = r.client.Ping(r.ctx).Result(); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	r.replicas, err = r.getConnectedSlaves()

	return err
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return r.features
}

// 获取主从模式下 从机的个数
func (r *StateStore) getConnectedSlaves() (int, error) {
	//# Server
	//redis_version:6.2.6
	//redis_git_sha1:00000000
	//redis_git_dirty:0
	//redis_build_id:b61f37314a089f19
	//redis_mode:standalone
	//os:Linux 3.10.0-862.el7.x86_64 x86_64
	//arch_bits:64
	//multiplexing_api:epoll
	//atomicvar_api:atomic-builtin
	//gcc_version:10.2.1
	//process_id:1
	//process_supervised:no
	//run_id:19b1c5e0dfcd029949a01958416f4b9e2da3943b
	//tcp_port:6379
	//server_time_usec:1640249478494095
	//uptime_in_seconds:19131
	//uptime_in_days:0
	//hz:10
	//configured_hz:10
	//lru_clock:12859526
	//executable:/data/redis-server
	//config_file:/usr/local/etc/redis/redis.conf
	//io_threads_active:0
	//
	//# Clients
	//connected_clients:1
	//cluster_connections:0
	//maxclients:10000
	//client_recent_max_input_buffer:24
	//client_recent_max_output_buffer:0
	//blocked_clients:0
	//tracking_clients:0
	//clients_in_timeout_table:0
	//
	//# Memory
	//used_memory:873848
	//used_memory_human:853.37K
	//used_memory_rss:4829184
	//used_memory_rss_human:4.61M
	//used_memory_peak:955032
	//used_memory_peak_human:932.65K
	//used_memory_peak_perc:91.50%
	//used_memory_overhead:830592
	//used_memory_startup:810080
	//used_memory_dataset:43256
	//used_memory_dataset_perc:67.83%
	//allocator_allocated:908984
	//allocator_active:1187840
	//allocator_resident:4001792
	//total_system_memory:16657145856
	//total_system_memory_human:15.51G
	//used_memory_lua:37888
	//used_memory_lua_human:37.00K
	//used_memory_scripts:0
	//used_memory_scripts_human:0B
	//number_of_cached_scripts:0
	//maxmemory:0
	//maxmemory_human:0B
	//maxmemory_policy:noeviction
	//allocator_frag_ratio:1.31
	//allocator_frag_bytes:278856
	//allocator_rss_ratio:3.37
	//allocator_rss_bytes:2813952
	//rss_overhead_ratio:1.21
	//rss_overhead_bytes:827392
	//mem_fragmentation_ratio:5.81
	//mem_fragmentation_bytes:3998096
	//mem_not_counted_for_evict:4
	//mem_replication_backlog:0
	//mem_clients_slaves:0
	//mem_clients_normal:20504
	//mem_aof_buffer:8
	//mem_allocator:jemalloc-5.1.0
	//active_defrag_running:0
	//lazyfree_pending_objects:0
	//lazyfreed_objects:0
	//
	//# Persistence
	//loading:0
	//current_cow_size:0
	//current_cow_size_age:0
	//current_fork_perc:0.00
	//current_save_keys_processed:0
	//current_save_keys_total:0
	//rdb_changes_since_last_save:0
	//rdb_bgsave_in_progress:0
	//rdb_last_save_time:1640230347
	//rdb_last_bgsave_status:ok
	//rdb_last_bgsave_time_sec:-1
	//rdb_current_bgsave_time_sec:-1
	//rdb_last_cow_size:0
	//aof_enabled:1
	//aof_rewrite_in_progress:0
	//aof_rewrite_scheduled:0
	//aof_last_rewrite_time_sec:-1
	//aof_current_rewrite_time_sec:-1
	//aof_last_bgrewrite_status:ok
	//aof_last_write_status:ok
	//aof_last_cow_size:0
	//module_fork_in_progress:0
	//module_fork_last_cow_size:0
	//aof_current_size:0
	//aof_base_size:0
	//aof_pending_rewrite:0
	//aof_buffer_length:0
	//aof_rewrite_buffer_length:0
	//aof_pending_bio_fsync:0
	//aof_delayed_fsync:0
	//
	//# Stats
	//total_connections_received:19
	//total_commands_processed:56
	//instantaneous_ops_per_sec:0
	//total_net_input_bytes:1413
	//total_net_output_bytes:6784
	//instantaneous_input_kbps:0.00
	//instantaneous_output_kbps:0.00
	//rejected_connections:0
	//sync_full:0
	//sync_partial_ok:0
	//sync_partial_err:0
	//expired_keys:0
	//expired_stale_perc:0.00
	//expired_time_cap_reached_count:0
	//expire_cycle_cpu_milliseconds:404
	//evicted_keys:0
	//keyspace_hits:0
	//keyspace_misses:0
	//pubsub_channels:0
	//pubsub_patterns:0
	//latest_fork_usec:0
	//total_forks:0
	//migrate_cached_sockets:0
	//slave_expires_tracked_keys:0
	//active_defrag_hits:0
	//active_defrag_misses:0
	//active_defrag_key_hits:0
	//active_defrag_key_misses:0
	//tracking_total_keys:0
	//tracking_total_items:0
	//tracking_total_prefixes:0
	//unexpected_error_replies:0
	//total_error_replies:4
	//dump_payload_sanitizations:0
	//total_reads_processed:79
	//total_writes_processed:60
	//io_threaded_reads_processed:0
	//io_threaded_writes_processed:0
	//
	//# Replication
	//role:master
	//connected_slaves:0
	//master_failover_state:no-failover
	//master_replid:a880e163c07658ce1211a3716cb7e5af1d744775
	//master_replid2:0000000000000000000000000000000000000000
	//master_repl_offset:0
	//second_repl_offset:-1
	//repl_backlog_active:0
	//repl_backlog_size:1048576
	//repl_backlog_first_byte_offset:0
	//repl_backlog_histlen:0
	//
	//# CPU
	//used_cpu_sys:11.093634
	//used_cpu_user:14.692364
	//used_cpu_sys_children:0.000000
	//used_cpu_user_children:0.000000
	//used_cpu_sys_main_thread:11.091609
	//used_cpu_user_main_thread:14.688521
	//
	//# Modules
	//
	//# Errorstats
	//errorstat_NOAUTH:count=4
	//
	//# Cluster
	//cluster_enabled:0
	//
	//# Keyspace
	res, err := r.client.Do(r.ctx, "INFO", "replication").Result()
	if err != nil {
		return 0, err
	}

	// Response example: https://redis.io/commands/info#return-value
	// # Replication\r\nrole:master\r\nconnected_slaves:1\r\n
	//// Unquote 将 s 解释为单引号、双引号。
	//// 或反引号的Go字符串字面，返回s引号的字符串值。
	//// s的引号。 (如果s是单引号，它就是一个Go
	//// (如果s是单引号的，它就是一个Go的字符字面；Unquote返回相应的
	//// 的单字符字符串）。
	s, _ := strconv.Unquote(fmt.Sprintf("%q", res))
	if len(s) == 0 {
		return 0, nil
	}

	return r.parseConnectedSlaves(s), nil
}

// 解析主从模式下 从机的个数
func (r *StateStore) parseConnectedSlaves(res string) int {
	//# Replication
	//role:master
	//connected_slaves:0
	//master_replid:c8704e428db1a3a831972fd1fa4a6ed9804feb0d
	//master_replid2:0000000000000000000000000000000000000000
	//master_repl_offset:0
	//second_repl_offset:-1
	//repl_backlog_active:0
	//repl_backlog_size:1048576
	//repl_backlog_first_byte_offset:0
	//repl_backlog_histlen:0
	infos := strings.Split(res, infoReplicationDelimiter)
	for _, info := range infos {
		if strings.Contains(info, connectedSlavesReplicas) {
			parsedReplicas, _ := strconv.ParseUint(info[len(connectedSlavesReplicas):], 10, 32)

			return int(parsedReplicas)
		}
	}

	return 0
}

func (r *StateStore) deleteValue(req *state.DeleteRequest) error {
	if req.ETag == nil {
		etag := "0"
		req.ETag = &etag
	}
	_, err := r.client.Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag).Result()
	if err != nil {
		return state.NewETagError(state.ETagMismatch, err)
	}

	return nil
}

// Delete performs a delete operation.
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	return state.DeleteWithOptions(r.deleteValue, req)
}

func (r *StateStore) directGet(req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.Do(r.ctx, "GET", req.Key).Result()
	if err != nil {
		return nil, err
	}

	if res == nil {
		return &state.GetResponse{}, nil
	}

	s, _ := strconv.Unquote(fmt.Sprintf("%q", res))

	return &state.GetResponse{
		Data: []byte(s),
	}, nil
}

// Get 从redis中检索带有密钥的状态。
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.Do(r.ctx, "HGETALL", req.Key).Result() // 倾向于带有ETags的值
	if err != nil {
		return r.directGet(req) // 回落到原来的得到，以便向后兼容。
	}
	if res == nil {
		return &state.GetResponse{}, nil
	}
	vals := res.([]interface{})
	if len(vals) == 0 {
		return &state.GetResponse{}, nil
	}

	data, version, err := r.getKeyVersion(vals)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: []byte(data),
		ETag: version,
	}, nil
}
// 真正在状态存储的方法
func (r *StateStore) setValue(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	ver, err := r.parseETag(req)
	if err != nil {
		return err
	}
	ttl, err := r.parseTTL(req)
	if err != nil {
		return fmt.Errorf("未能从元数据中解析ttl: %s", err)
	}
	// apply global TTL
	if ttl == nil {
		ttl = r.metadata.ttlInSeconds
	}
	// 序列化value
	bt, _ := utils.Marshal(req.Value, r.json.Marshal)

	firstWrite := 1
	if req.Options.Concurrency == state.FirstWrite {
		firstWrite = 0
	}
	// 调用lua 脚本，设置值，同时设置版本号【ETAG】        1个KEYS参数  [KEY...] [ARGV...]
	_, err = r.client.Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt, firstWrite).Result()
	// todo 第一次设置了以后，version=1 ,第二次调用，因为version存在会直接反回错误
	// 获取 key.version
	// if type(version) == dict{
	//    delete key
	//}
	//获取key.first-write  							1 demo 12 value 1
	// if version 不存在| type(version) == dict | version=="" | var1 == ARGV[1] | (not var2 and ARGV[1] == "0")
	//  	"HSET", demo, "data", ARGV[2]
	//   	if ARGV[3] == "0"
	//  		"HSET", demo , "first-write" 0,
	//   	HINCRBY", demo, "version", 1
	// else
	// 		return error("failed to set key " .. KEYS[1])

	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("failed to set key %s: %s", req.Key, err)
	}

	if ttl != nil && *ttl > 0 {
		_, err = r.client.Do(r.ctx, "EXPIRE", req.Key, *ttl).Result()
		if err != nil {
			return fmt.Errorf("failed to set key %s ttl: %s", req.Key, err)
		}
	}

	if ttl != nil && *ttl <= 0 {
		_, err = r.client.Do(r.ctx, "PERSIST", req.Key).Result()
		if err != nil {
			return fmt.Errorf("failed to persist key %s: %s", req.Key, err)
		}
	}

	if req.Options.Consistency == state.Strong && r.replicas > 0 {
		_, err = r.client.Do(r.ctx, "WAIT", r.replicas, 1000).Result()
		if err != nil {
			return fmt.Errorf("redis waiting for %v replicas to acknowledge write, err: %s", r.replicas, err.Error())
		}
	}

	return nil
}

// Set 将状态保存到redis。
func (r *StateStore) Set(req *state.SetRequest) error {
	return state.SetWithOptions(r.setValue, req)
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (r *StateStore) Multi(request *state.TransactionalStateRequest) error {
	pipe := r.client.TxPipeline()
	for _, o := range request.Operations {
		//nolint:golint,nestif
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			ver, err := r.parseETag(&req)
			if err != nil {
				return err
			}
			ttl, err := r.parseTTL(&req)
			if err != nil {
				return fmt.Errorf("failed to parse ttl from metadata: %s", err)
			}
			// apply global TTL
			if ttl == nil {
				ttl = r.metadata.ttlInSeconds
			}

			bt, _ := utils.Marshal(req.Value, r.json.Marshal)
			pipe.Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt)
			if ttl != nil && *ttl > 0 {
				pipe.Do(r.ctx, "EXPIRE", req.Key, *ttl)
			}
			if ttl != nil && *ttl <= 0 {
				pipe.Do(r.ctx, "PERSIST", req.Key)
			}
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			if req.ETag == nil {
				etag := "0"
				req.ETag = &etag
			}
			pipe.Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag)
		}
	}

	_, err := pipe.Exec(r.ctx)

	return err
}

func (r *StateStore) getKeyVersion(vals []interface{}) (data string, version *string, err error) {
	seenData := false
	seenVersion := false
	for i := 0; i < len(vals); i += 2 {
		field, _ := strconv.Unquote(fmt.Sprintf("%q", vals[i]))
		switch field {
		case "data":
			data, _ = strconv.Unquote(fmt.Sprintf("%q", vals[i+1]))
			seenData = true
		case "version":
			versionVal, _ := strconv.Unquote(fmt.Sprintf("%q", vals[i+1]))
			version = ptr.String(versionVal)
			seenVersion = true
		}
	}
	if !seenData || !seenVersion {
		return "", nil, errors.New("required hash field 'data' or 'version' was not found")
	}

	return data, version, nil
}
// 解析用书set数据  设置的ETAG
func (r *StateStore) parseETag(req *state.SetRequest) (int, error) {
	if req.Options.Concurrency == state.LastWrite || req.ETag == nil || *req.ETag == "" { // 最后一次写,没有设置ETAG
		return 0, nil
	}
	ver, err := strconv.Atoi(*req.ETag)// etag 必须是数字型字符串
	if err != nil {
		return -1, state.NewETagError(state.ETagInvalid, err)
	}

	return ver, nil
}

func (r *StateStore) parseTTL(req *state.SetRequest) (*int, error) {
	if val, ok := req.Metadata[ttlInSeconds]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return nil, err
		}
		ttl := int(parsedVal)

		return &ttl, nil
	}

	return nil, nil
}

func (r *StateStore) Close() error {
	r.cancel()

	return r.client.Close()
}
