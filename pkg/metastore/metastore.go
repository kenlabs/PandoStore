package metastore

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dtsync "github.com/ipfs/go-datastore/sync"
)

type MetaStore struct {
	mds *dtsync.MutexDatastore
}

func New(mds *dtsync.MutexDatastore) (*MetaStore, error) {
	return &MetaStore{mds: mds}, nil
}

func (ms *MetaStore) CheckExisted(ctx context.Context, key cid.Cid) (bool, error) {
	return ms.mds.Has(ctx, datastore.NewKey(key.String()))
}

func (ms *MetaStore) Put(ctx context.Context, key cid.Cid, val []byte) error {
	return ms.mds.Put(ctx, datastore.NewKey(key.String()), val)
}

func (ms *MetaStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	return ms.mds.Get(ctx, datastore.NewKey(key.String()))
}

func (ms *MetaStore) Close() error {
	return nil
}
