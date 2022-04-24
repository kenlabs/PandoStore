package metastore

import (
	"context"
	"github.com/ipfs/go-datastore"
)

type MetaStore struct {
	ds datastore.Batching
}

func New(ds datastore.Batching) (*MetaStore, error) {
	return &MetaStore{ds: ds}, nil
}

func (ms *MetaStore) CheckExisted(ctx context.Context, key string) (bool, error) {
	return ms.ds.Has(ctx, datastore.NewKey(key))
}

func (ms *MetaStore) Put(ctx context.Context, key string, val []byte) error {
	return ms.ds.Put(ctx, datastore.NewKey(key), val)
}
