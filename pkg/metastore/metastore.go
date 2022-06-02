package metastore

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	dtsync "github.com/ipfs/go-datastore/sync"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

var MetaPrefix = datastore.NewKey("meta")

type MetaStore struct {
	mds datastore.Batching
}

func New(mds *dtsync.MutexDatastore) (*MetaStore, error) {
	metaDs := dsns.Wrap(mds, MetaPrefix)
	return &MetaStore{mds: metaDs}, nil
}

func (ms *MetaStore) CheckExisted(ctx context.Context, key cid.Cid) (bool, error) {
	return ms.mds.Has(ctx, dshelp.MultihashToDsKey(key.Hash()))
}

func (ms *MetaStore) Put(ctx context.Context, key cid.Cid, val []byte) error {
	return ms.mds.Put(ctx, dshelp.MultihashToDsKey(key.Hash()), val)
}

func (ms *MetaStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	return ms.mds.Get(ctx, dshelp.MultihashToDsKey(key.Hash()))
}

func (ms *MetaStore) Close() error {
	return nil
}
