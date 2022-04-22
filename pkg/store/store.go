package store

import (
	"PandoStore/pkg/metastore"
	"PandoStore/pkg/providerstore"
	"PandoStore/pkg/snapshotstore"
	"PandoStore/pkg/types"
	"context"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)

type PandoStore struct {
	// for provider update in snapshot
	mutex            map[peer.ID]*sync.Mutex
	state            types.StoreState
	snapshotDone     chan struct{}
	taskInProcessing sync.WaitGroup
	metaStore        *metastore.MetaStore
	providerStore    *providerstore.ProviderStore
	snapShotStore    *snapshotstore.SnapShotStore
	waitForSnapshot  map[peer.ID][]cid.Cid
}

func NewPandoStore(ms *metastore.MetaStore, ps *providerstore.ProviderStore, sstore *snapshotstore.SnapShotStore) (*PandoStore, error) {
	return &PandoStore{
		metaStore:     ms,
		providerStore: ps,
		snapShotStore: sstore,
	}, nil
}

func (ps *PandoStore) Store(ctx context.Context, key string, val []byte, provider peer.ID, metaContext []byte, prev []byte) error {
	if ps.state != types.Working {
		// wait snapshot finished
		<-ps.snapshotDone
	}
	ps.taskInProcessing.Add(1)
	defer ps.taskInProcessing.Done()

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

func (ps *PandoStore) updateCache(provider peer.ID, c cid.Cid) error {
	mux, ok := ps.mutex[provider]
	if !ok {
		ps.mutex[provider] = new(sync.Mutex)
		mux = ps.mutex[provider]
	}
	mux.Lock()
	defer mux.Unlock()

	ps.waitForSnapshot[provider] = append(ps.waitForSnapshot[provider], c)
	return nil
}

func (ps *PandoStore) generateSnapShot(ctx context.Context, update map[peer.ID][]cid.Cid) error {
	// avoid closed channel
	ps.snapshotDone = make(chan struct{})
	ps.state = types.SnapShoting
	// wait processing tasks finish
	ps.taskInProcessing.Wait()

	root, err := ps.providerStore.ProviderStateRoot()
	if err != nil {
		return err
	}
	// gen new snapshot
	c, snapshot, err := ps.snapShotStore.GenerateSnapShot(ctx, update, root)
	if err != nil {
		return err
	}
	// update provider state
	err = ps.providerStore.ProvidersUpdateMeta(ctx, update, snapshot, c)
	if err != nil {
		return err
	}

	// release blocked tasks
	close(ps.snapshotDone)
	ps.state = types.Working

	return nil
}
