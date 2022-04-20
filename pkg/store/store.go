package store

import (
	"PandoStore/pkg/metastore"
	"PandoStore/pkg/providerstore"
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
)

type PandoStore struct {
	//ds datastore.Batching
	metaStore     *metastore.MetaStore
	providerStore *providerstore.ProviderStore
}

func NewPandoStore(ms *metastore.MetaStore, ps *providerstore.ProviderStore) (*PandoStore, error) {
	return &PandoStore{
		metaStore:     ms,
		providerStore: ps,
	}, nil
}

func (ps *PandoStore) Store(ctx context.Context, key string, val []byte, provider peer.ID, metaContext []byte, prev []byte) error {
	// key has existed or failed to check
	if err := ps.metaStore.CheckExisted(ctx, key); err != nil {
		return err
	}

	// save meta data
	if err := ps.metaStore.Put(ctx, key, val); err != nil {
		return err
	}

	// update meta state and provider info
	if err := ps.providerStore.ProviderAddMeta(ctx, provider, key, metaContext); err != nil {
		return err
	}

	return nil
}
