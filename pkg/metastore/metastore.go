package metastore

import (
	"context"
	"fmt"
	"github.com/ipfs/go-datastore"
)

type MetaStore struct {
	ds datastore.Batching
}

func New(ds datastore.Batching) (*MetaStore, error) {
	return &MetaStore{ds: ds}, nil
}

func (ms *MetaStore) CheckExisted(ctx context.Context, key string) error {
	if existed, err := ms.ds.Has(ctx, datastore.NewKey(key)); existed && err == nil {
		return KeyHasExisted
	} else if err != nil {
		return fmt.Errorf("failed to check, err: %v", err)
	}
	return nil
}

func (ms *MetaStore) Put(ctx context.Context, key string, val []byte) error {
	return ms.ds.Put(ctx, datastore.NewKey(key), val)
}
