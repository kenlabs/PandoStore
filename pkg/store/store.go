package store

import (
	"PandoStore/pkg/config"
	"PandoStore/pkg/metastore"
	"PandoStore/pkg/snapshotstore"
	"PandoStore/pkg/statestore"
	"PandoStore/pkg/system"
	"PandoStore/pkg/types/store"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/ipfs/go-datastore"
	dtsync "github.com/ipfs/go-datastore/sync"
	dataStoreFactory "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"

	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var log = logging.Logger("PandoStore")

type PandoStore struct {
	cfg *config.StoreConfig
	// for provider update in snapshot
	providerMutex    map[peer.ID]*sync.Mutex
	stateMutex       sync.RWMutex
	state            store.StoreState
	snapshotDone     chan struct{}
	taskInProcessing sync.WaitGroup
	basicDS          datastore.Batching
	metaStore        *metastore.MetaStore
	metaStateStore   *statestore.MetaStateStore
	SnapShotStore    *snapshotstore.SnapShotStore
	waitForSnapshot  map[peer.ID][]cid.Cid
	ctx              context.Context
	cncl             context.CancelFunc
}

func NewStoreFromDatastore(ctx context.Context, ds datastore.Batching, cfg *config.StoreConfig) (*PandoStore, error) {
	childCtx, cncl := context.WithCancel(ctx)
	mutexDatastore := dtsync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mutexDatastore)
	cs := cbor.NewCborStore(bs)
	as := adt.WrapStore(childCtx, cs)
	metaStore, _ := metastore.New(mutexDatastore)
	stateStore, err := statestore.New(childCtx, mutexDatastore, as)
	if err != nil {
		cncl()
		return nil, err
	}
	snapStore, _ := snapshotstore.NewStore(childCtx, mutexDatastore, cs)

	s := &PandoStore{
		ctx:  childCtx,
		cncl: cncl,
		//stateMutex:      sync.RWMutex{},
		providerMutex:   make(map[peer.ID]*sync.Mutex),
		waitForSnapshot: make(map[peer.ID][]cid.Cid),
		basicDS:         mutexDatastore,
		metaStore:       metaStore,
		metaStateStore:  stateStore,
		SnapShotStore:   snapStore,
		cfg:             cfg,
	}
	err = s.run()
	if err != nil {
		cncl()
		return nil, err
	}

	return s, nil

}

func NewStoreFromConfig(ctx context.Context, cfg *config.StoreConfig) (*PandoStore, error) {
	childCtx, cncl := context.WithCancel(ctx)
	if cfg.Type != "levelds" {
		cncl()
		return nil, fmt.Errorf("only levelds datastore type supported")
	}
	if cfg.Dir == "" {
		cfg.Dir = config.DefaultStoreDir
	}
	dataStoreDir := filepath.Join(cfg.StoreRoot, cfg.Dir)
	dataStoreDirExists, err := system.IsDirExists(dataStoreDir)
	if !dataStoreDirExists {
		err := os.MkdirAll(dataStoreDir, 0755)
		if err != nil {
			cncl()
			return nil, err
		}
	}

	writable, err := system.IsDirWritable(dataStoreDir)
	if err != nil {
		cncl()
		return nil, err
	}
	if !writable {
		cncl()
		return nil, err
	}
	dataStore, err := dataStoreFactory.NewDatastore(dataStoreDir, nil)
	if err != nil {
		cncl()
		return nil, err
	}
	mutexDatastore := dtsync.MutexWrap(dataStore)
	bs := blockstore.NewBlockstore(mutexDatastore)
	cs := cbor.NewCborStore(bs)
	as := adt.WrapStore(childCtx, cs)
	metaStore, _ := metastore.New(mutexDatastore)
	stateStore, err := statestore.New(childCtx, mutexDatastore, as)
	if err != nil {
		cncl()
		return nil, err
	}
	snapStore, _ := snapshotstore.NewStore(childCtx, mutexDatastore, cs)

	s := &PandoStore{
		ctx:             childCtx,
		cncl:            cncl,
		providerMutex:   make(map[peer.ID]*sync.Mutex),
		waitForSnapshot: make(map[peer.ID][]cid.Cid),
		basicDS:         mutexDatastore,
		metaStore:       metaStore,
		metaStateStore:  stateStore,
		SnapShotStore:   snapStore,
		cfg:             cfg,
	}
	err = s.run()
	if err != nil {
		cncl()
		return nil, err
	}

	return s, nil
}

func (ps *PandoStore) Store(ctx context.Context, key string, val []byte, provider peer.ID, metaContext []byte) error {
	ps.stateMutex.RLock()
	if ps.state != store.Working {
		if ps.state == store.SnapShoting {
			ps.stateMutex.Unlock()
			// wait snapshot finished
			<-ps.snapshotDone
		} else if ps.state == store.Closing {
			ps.stateMutex.Unlock()
			return fmt.Errorf("pandostore is closing, failed to store: %s", key)
		} else {
			ps.stateMutex.Unlock()
			return fmt.Errorf("unknown work state: %v", ps.state)
		}

	} else {
		ps.stateMutex.RUnlock()
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

func (ps *PandoStore) Get(ctx context.Context, key string) ([]byte, error) {
	return ps.metaStore.Get(ctx, key)
}

func (ps *PandoStore) updateCache(provider peer.ID, c cid.Cid) error {
	mux, ok := ps.providerMutex[provider]
	if !ok {
		ps.providerMutex[provider] = new(sync.Mutex)
		mux = ps.providerMutex[provider]
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
	ps.stateMutex.Lock()
	ps.state = store.SnapShoting
	ps.stateMutex.Unlock()
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
	ps.stateMutex.Lock()
	ps.state = store.Working
	ps.stateMutex.Unlock()

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

func (ps *PandoStore) Close() error {
	// block incoming store/get
	ps.stateMutex.Lock()
	if ps.state == store.Closing {
		log.Warnf("close repeatly....")
		return nil
	}
	ps.state = store.Closing
	ps.stateMutex.Unlock()

	// wait processing tasks
	ps.taskInProcessing.Wait()
	err := ps.metaStore.Close()
	if err != nil {
		return err
	}
	err = ps.metaStateStore.Close()
	if err != nil {
		return err
	}
	err = ps.SnapShotStore.Close()
	if err != nil {
		return err
	}
	// close context
	ps.cncl()
	return ps.basicDS.Close()
}
