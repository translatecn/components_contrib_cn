// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// Store 是一个对存储进行操作的接口。
type Store interface {
	BulkStore
	Init(metadata Metadata) error
	Features() []Feature
	Delete(req *DeleteRequest) error
	Get(req *GetRequest) (*GetResponse, error)
	Set(req *SetRequest) error
	Ping() error
}

// BulkStore 是一个对存储进行批量操作的接口。
type BulkStore interface {
	BulkDelete(req []DeleteRequest) error
	BulkGet(req []GetRequest) (bool, []BulkGetResponse, error)
	BulkSet(req []SetRequest) error
}

// DefaultBulkStore 是BulkStore的一个默认实现。
type DefaultBulkStore struct {
	s Store
}

// NewDefaultBulkStore  建立一个默认的批量存储。
func NewDefaultBulkStore(store Store) DefaultBulkStore {
	defaultBulkStore := DefaultBulkStore{}
	defaultBulkStore.s = store

	return defaultBulkStore
}

// Features 返回被封装的存储空间的特征。
func (b *DefaultBulkStore) Features() []Feature {
	return b.s.Features()
}

// BulkGet 执行大量的获取操作。
func (b *DefaultBulkStore) BulkGet(req []GetRequest) (bool, []BulkGetResponse, error) {
	// 默认情况下，商店不支持批量获取，所以daprd将退回到一个一个地调用get()方法。
	return false, nil, nil
}

// BulkSet 执行一个大批量的保存操作。
func (b *DefaultBulkStore) BulkSet(req []SetRequest) error {
	for i := range req {
		err := b.s.Set(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// BulkDelete 执行一个批量删除操作。
func (b *DefaultBulkStore) BulkDelete(req []DeleteRequest) error {
	for i := range req {
		err := b.s.Delete(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// Querier 是一个执行查询的接口。
type Querier interface {
	Query(req *QueryRequest) (*QueryResponse, error)
}
