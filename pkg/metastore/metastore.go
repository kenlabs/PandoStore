package metastore

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
)

type MetaStore struct {
	ds datastore.Batching
}

func New(ds datastore.Batching) (*MetaStore, error) {
	return &MetaStore{ds: ds}, nil
}

func (ms *MetaStore) CheckExisted(ctx context.Context, key cid.Cid) (bool, error) {
	return ms.ds.Has(ctx, datastore.NewKey(key.String()))
}

func (ms *MetaStore) Put(ctx context.Context, key cid.Cid, val []byte) error {
	return ms.ds.Put(ctx, datastore.NewKey(key.String()), val)
}

func (ms *MetaStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	return ms.ds.Get(ctx, datastore.NewKey(key.String()))
}

func (ms *MetaStore) Close() error {
	return nil
}
