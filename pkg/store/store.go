package store

import (
	"PandoStore/pkg/config"
	"PandoStore/pkg/metastore"
	"PandoStore/pkg/snapshotstore"
	"PandoStore/pkg/statestore"
	"PandoStore/pkg/types/store"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

var log = logging.Logger("PandoStore")

type PandoStore struct {
	cfg *config.StoreConfig
	// for provider update in snapshot
	mutex            map[peer.ID]*sync.Mutex
	state            store.StoreState
	snapshotDone     chan struct{}
	taskInProcessing sync.WaitGroup
	metaStore        *metastore.MetaStore
	metaStateStore   *statestore.MetaStateStore
	SnapShotStore    *snapshotstore.SnapShotStore
	waitForSnapshot  map[peer.ID][]cid.Cid
	ctx              context.Context
	cncl             context.CancelFunc
}

func NewPandoStore(ctx context.Context, ms *metastore.MetaStore, ps *statestore.MetaStateStore, sstore *snapshotstore.SnapShotStore, cfg *config.StoreConfig) (*PandoStore, error) {
	childCtx, cncl := context.WithCancel(ctx)
	store := &PandoStore{
		ctx:             childCtx,
		cncl:            cncl,
		mutex:           make(map[peer.ID]*sync.Mutex),
		waitForSnapshot: make(map[peer.ID][]cid.Cid),
		metaStore:       ms,
		metaStateStore:  ps,
		SnapShotStore:   sstore,
		cfg:             cfg,
	}
	err := store.run()
	if err != nil {
		return nil, err
	}

	return store, nil

}

func (ps *PandoStore) Store(ctx context.Context, key string, val []byte, provider peer.ID, metaContext []byte, prev []byte) error {
	if ps.state != store.Working {
		// wait snapshot finished
		<-ps.snapshotDone
	}
	ps.taskInProcessing.Add(1)
	defer ps.taskInProcessing.Done()

	c, err := cid.Decode(key)
	if err != nil {
		return err
	}

	// key has existed or failed to check
	if ok, err := ps.metaStore.CheckExisted(ctx, key); err != nil || ok {
		return fmt.Errorf("key has existed or failed to check, err: %v", err)
	}

	// save meta data
	if err := ps.metaStore.Put(ctx, key, val); err != nil {
		return err
	}

	// update meta state and provider info
	if err := ps.metaStateStore.ProviderAddMeta(ctx, provider, key, metaContext); err != nil {
		return err
	}

	if err := ps.updateCache(provider, c); err != nil {
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

func (ps *PandoStore) generateSnapShot(ctx context.Context) error {
	// skip
	if len(ps.waitForSnapshot) == 0 {
		return nil
	}
	// avoid closed channel
	ps.snapshotDone = make(chan struct{})
	ps.state = store.SnapShoting
	// wait processing tasks finish
	ps.taskInProcessing.Wait()

	root, err := ps.metaStateStore.MetaStateRoot()
	if err != nil {
		return err
	}
	// gen new snapshot
	c, snapshot, err := ps.SnapShotStore.GenerateSnapShot(ctx, ps.waitForSnapshot, root)
	if err != nil {
		return err
	}
	// update provider state
	err = ps.metaStateStore.ProvidersUpdateMeta(ctx, ps.waitForSnapshot, snapshot, c)
	if err != nil {
		return err
	}

	// clean cache
	log.Infof("clean snapshot cache")
	ps.waitForSnapshot = make(map[peer.ID][]cid.Cid)
	// release blocked tasks
	close(ps.snapshotDone)
	ps.state = store.Working

	return nil
}

func (ps *PandoStore) MetaInclusion(ctx context.Context, c cid.Cid) (*store.MetaInclusion, error) {
	res := &store.MetaInclusion{}
	res.ID = c

	ok, err := ps.metaStore.CheckExisted(ctx, c.String())
	if err != nil {
		return nil, err
	}
	if !ok {
		res.InPando = false
		return res, nil
	}

	info, err := ps.metaStateStore.GetMetaInfo(ctx, c)
	if err != nil {
		log.Errorf("meta existed but failed to get meta state, err: %v", err)
		return nil, err
	}
	res.InPando = true
	res.Context = info.Context
	p, err := peer.Decode(info.ProviderID)
	if err != nil {
		return nil, err
	}
	res.Provider = p

	if info.SnapShotCid != "" {
		scid, err := cid.Decode(info.SnapShotCid)
		if err != nil {
			return nil, err
		}
		res.InSnapShot = true
		res.SnapShotID = scid
		res.SnapShotHeight = info.SnapShotHeight
	}
	return res, nil

}

func (ps *PandoStore) run() error {
	interval, err := time.ParseDuration(ps.cfg.SnapShotInterval)
	if err != nil {
		return err
	}

	go func() {
		for range time.NewTicker(interval).C {
			err = ps.generateSnapShot(ps.ctx)
			if err != nil {
				log.Errorf("failed to generate snapshot, closing PandoStore.... err: %v", err)
				ps.cncl()
				return
			}
		}
	}()

	return nil
}
