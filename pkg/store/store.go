package store

import (
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	lru "github.com/hashicorp/golang-lru"
	dtsync "github.com/ipfs/go-datastore/sync"
	dataStoreFactory "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/kenlabs/PandoStore/pkg"
	"github.com/kenlabs/PandoStore/pkg/config"
	"github.com/kenlabs/PandoStore/pkg/error"
	"github.com/kenlabs/PandoStore/pkg/metastore"
	"github.com/kenlabs/PandoStore/pkg/migrate"
	"github.com/kenlabs/PandoStore/pkg/snapshotstore"
	"github.com/kenlabs/PandoStore/pkg/statestore"
	"github.com/kenlabs/PandoStore/pkg/system"
	"github.com/kenlabs/PandoStore/pkg/types/store"

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
var _ pkg.PandoStore = &PandoStore{}

func getEvictFunction(ctx context.Context, store *metastore.MetaStore) func(key interface{}, value interface{}) {
	return func(key interface{}, value interface{}) {
		log.Debugf("entering the evict callback")
		_key, ok := key.(cid.Cid)
		if !ok {
			log.Errorf("wrong kind of key, key: %v", key)
			return
		}
		_value, ok := value.([]byte)
		if !ok {
			log.Errorf("wrong kind of value, value: %v", value)
			return
		}
		err := store.Put(ctx, _key, _value)
		if err != nil {
			log.Errorf("failed to save the evicted data from lru cache, err: %v", err)
		}
	}
}

type PandoStore struct {
	cfg   *config.StoreConfig
	cache *lru.Cache
	// for provider update in snapshot
	providerMutex    map[peer.ID]*sync.Mutex
	stateMutex       sync.RWMutex
	state            store.StoreState
	snapshotDone     chan struct{}
	taskInProcessing sync.WaitGroup
	BasicDS          *dtsync.MutexDatastore
	metaStore        *metastore.MetaStore
	StateStore       *statestore.MetaStateStore
	snapShotStore    *snapshotstore.SnapShotStore
	waitForSnapshot  map[peer.ID][]cid.Cid
	ctx              context.Context
	cncl             context.CancelFunc
}

func NewStoreFromDatastore(ctx context.Context, mds *dtsync.MutexDatastore, cfg *config.StoreConfig) (*PandoStore, error) {
	childCtx, cncl := context.WithCancel(ctx)
	bs := blockstore.NewBlockstore(mds)
	cs := cbor.NewCborStore(bs)
	as := adt.WrapStore(childCtx, cs)
	metaStore, _ := metastore.New(mds)
	stateStore, err := statestore.New(childCtx, mds, as)
	if err != nil {
		cncl()
		return nil, err
	}
	snapStore, _ := snapshotstore.NewStore(childCtx, mds, cs)
	cache, err := lru.NewWithEvict(cfg.CacheSize, getEvictFunction(childCtx, metaStore))
	if err != nil {
		cncl()
		return nil, err
	}

	s := &PandoStore{
		ctx:             childCtx,
		cache:           cache,
		cncl:            cncl,
		providerMutex:   make(map[peer.ID]*sync.Mutex),
		waitForSnapshot: make(map[peer.ID][]cid.Cid),
		BasicDS:         mds,
		metaStore:       metaStore,
		StateStore:      stateStore,
		snapShotStore:   snapStore,
		cfg:             cfg,
	}
	err = s.run()
	if err != nil {
		cncl()
		return nil, err
	}

	return s, nil

}

func NewStoreFromConfig(ctx context.Context, cfg *config.StoreConfig) (pkg.PandoStore, error) {
	//childCtx, cncl := context.WithCancel(ctx)
	//defer cncl()
	if cfg.Type != "levelds" {
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
			return nil, err
		}
	}

	writable, err := system.IsDirWritable(dataStoreDir)
	if err != nil {
		return nil, err
	}
	if !writable {
		return nil, err
	}
	version, err := pkg.CheckVersion(dataStoreDir)
	if err != nil {
		return nil, err
	}
	// migrate
	if version != pkg.CurrentVersion {
		err = migrate.Migrate(version, pkg.CurrentVersion, dataStoreDir, false)
		if err != nil {
			return nil, err
		}
	}
	dataStore, err := dataStoreFactory.NewDatastore(dataStoreDir, nil)
	if err != nil {
		return nil, err
	}
	mutexDatastore := dtsync.MutexWrap(dataStore)

	return NewStoreFromDatastore(ctx, mutexDatastore, cfg)
	//bs := blockstore.NewBlockstore(mutexDatastore)
	//cs := cbor.NewCborStore(bs)
	//as := adt.WrapStore(childCtx, cs)
	//metaStore, _ := metastore.New(mutexDatastore)
	//stateStore, err := statestore.New(childCtx, mutexDatastore, as)
	//if err != nil {
	//	return nil, err
	//}
	//snapStore, _ := snapshotstore.NewStore(childCtx, mutexDatastore, cs)
	//
	//s := &PandoStore{
	//	ctx:             childCtx,
	//	cncl:            cncl,
	//	providerMutex:   make(map[peer.ID]*sync.Mutex),
	//	waitForSnapshot: make(map[peer.ID][]cid.Cid),
	//	BasicDS:         mutexDatastore,
	//	metaStore:       metaStore,
	//	StateStore:      stateStore,
	//	snapShotStore:   snapStore,
	//	cfg:             cfg,
	//}
	//err = s.run()
	//if err != nil {
	//	return nil, err
	//}

	//return s, nil
}

func (ps *PandoStore) Store(ctx context.Context, key cid.Cid, val []byte, provider peer.ID, metaContext []byte) error {
	if !key.Defined() {
		return fmt.Errorf("invalid cid")
	}

	//ps.stateMutex.RLock()
	//if ps.state != store.Working {
	//	if ps.state == store.SnapShoting {
	//		ps.stateMutex.RUnlock()
	//		// wait snapshot finished
	//		<-ps.snapshotDone
	//	} else if ps.state == store.Closing {
	//		ps.stateMutex.RUnlock()
	//		return fmt.Errorf("pandostore is closing, failed to store: %s", key.String())
	//	} else {
	//		ps.stateMutex.RUnlock()
	//		return fmt.Errorf("unknown work state: %v", ps.state)
	//	}
	//
	//} else {
	//	ps.stateMutex.RUnlock()
	//}
	//ps.taskInProcessing.Add(1)
	err := ps.checkStatusAndAddTask()
	if err != nil {
		return err
	}
	defer ps.taskInProcessing.Done()

	// check cache
	if ok := ps.cache.Contains(key); ok {
		return storeError.KeyHasExisted
	}

	// check persisted store
	ok, err := ps.metaStore.CheckExisted(ctx, key)
	if err != nil {
		log.Errorf("failed to check whether data existed in metastore, err: %v", err)
		return err
	}
	if ok {
		return storeError.KeyHasExisted
	}

	// save meta data
	//if err := ps.metaStore.Put(ctx, key, val); err != nil {
	//	return err
	//}
	_ = ps.cache.Add(key, val)

	// update meta state and provider info
	err = ps.StateStore.AddMetaInfo(provider, key, metaContext)
	if err != nil {
		return err
	}

	if err := ps.updateSnapShotCache(provider, key); err != nil {
		return err
	}

	return nil
}

func (ps *PandoStore) Get(ctx context.Context, key cid.Cid) ([]byte, error) {
	if v, ok := ps.cache.Get(key); ok {
		data, ok := v.([]byte)
		if !ok {
			log.Errorf("wrong kind of data from cache, data: %v", v)
		} else {
			return data, nil
		}
	}
	return ps.metaStore.Get(ctx, key)
}

func (ps *PandoStore) updateSnapShotCache(provider peer.ID, c cid.Cid) error {
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
	err := ps.checkStatusAndGenSnapShot()
	if err != nil {
		return err
	}
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

	root, err := ps.StateStore.MetaStateRoot()
	if err != nil {
		return err
	}
	// gen new snapshot
	c, snapshot, err := ps.snapShotStore.GenerateSnapShot(ctx, ps.waitForSnapshot, root)
	if err != nil {
		return err
	}
	// update provider state
	_, err = ps.StateStore.ProvidersUpdateMeta(ctx, ps.waitForSnapshot, snapshot, c)
	if err != nil {
		return err
	}

	// clean cache
	log.Infof("clean updated metaState in SnapShot")
	//ps.waitForSnapshot = make(map[peer.ID][]cid.Cid)
	// release blocked tasks
	ps.stateMutex.Lock()
	close(ps.snapshotDone)
	ps.state = store.Working
	ps.stateMutex.Unlock()

	return nil
}

func (ps *PandoStore) MetaInclusion(ctx context.Context, c cid.Cid) (*store.MetaInclusion, error) {
	res := &store.MetaInclusion{}
	res.ID = c

	ok, err := ps.metaStore.CheckExisted(ctx, c)
	if err != nil {
		return nil, err
	}
	if !ok {
		res.InPando = false
		return res, nil
	}
	res.InPando = true
	// get metaState, but the state may be queued for hamt
	info, err := ps.StateStore.GetMetaInfo(ctx, c)
	if err != nil {
		log.Warnf("meta existed but failed to get meta state, err: %v", err)
		//return nil, err
	} else {
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
	}

	return res, nil

}

func (ps *PandoStore) SnapShotStore() *snapshotstore.SnapShotStore {
	return ps.snapShotStore
}

func (ps *PandoStore) run() error {
	interval, err := time.ParseDuration(ps.cfg.SnapShotInterval)
	if err != nil {
		return err
	}

	go func() {
		for range time.NewTicker(interval).C {
			select {
			case _ = <-ps.ctx.Done():
				return
			default:
			}
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
	err := ps.checkStatusAndClose()
	if err != nil {
		return err
	}
	// close context
	ps.cncl()
	// block until all cache is persisted
	ps.persistCache()
	// wait processing tasks
	ps.taskInProcessing.Wait()
	err = ps.metaStore.Close()
	if err != nil {
		return err
	}
	err = ps.StateStore.Close()
	if err != nil {
		return err
	}
	err = ps.snapShotStore.Close()
	if err != nil {
		return err
	}

	return ps.BasicDS.Close()
}

func (ps *PandoStore) checkStatusAndAddTask() error {
	// check and wait state is working
	for {
		ps.stateMutex.RLock()
		if ps.state != store.Working {
			if ps.state == store.SnapShoting {
				ps.stateMutex.RUnlock()
				// wait snapshot finished
				<-ps.snapshotDone
			} else if ps.state == store.Closing {
				ps.stateMutex.RUnlock()
				return storeError.StoreClosed
			} else {
				ps.stateMutex.RUnlock()
				return fmt.Errorf("unknown work state: %v", ps.state)
			}

		} else {
			// working state, add task and release lock
			ps.taskInProcessing.Add(1)
			ps.stateMutex.RUnlock()
			break
		}
	}
	return nil
}

func (ps *PandoStore) checkStatusAndGenSnapShot() error {
	// check and wait state is working
	for {
		ps.stateMutex.Lock()
		if ps.state != store.Working {
			if ps.state == store.SnapShoting {
				ps.stateMutex.Unlock()
				return fmt.Errorf("has been generating snapshot")
			} else if ps.state == store.Closing {
				ps.stateMutex.Unlock()
				log.Errorf("close repeatly.")
				return storeError.StoreClosed
			} else {
				ps.stateMutex.Unlock()
				return fmt.Errorf("unknown work state: %v", ps.state)
			}
		} else {
			// working state, add task and release lock
			ps.state = store.SnapShoting
			ps.stateMutex.Unlock()
			break
		}
	}
	return nil
}

func (ps *PandoStore) checkStatusAndClose() error {
	// check and wait state is working
	for {
		ps.stateMutex.Lock()
		if ps.state != store.Working {
			if ps.state == store.SnapShoting {
				ps.stateMutex.Unlock()
				// wait snapshot finished
				<-ps.snapshotDone
			} else if ps.state == store.Closing {
				ps.stateMutex.Unlock()
				log.Errorf("close repeatly.")
				return storeError.StoreClosed
			} else {
				ps.stateMutex.Unlock()
				return fmt.Errorf("unknown work state: %v", ps.state)
			}

		} else {
			// working state, add task and release lock
			ps.state = store.Closing
			ps.stateMutex.Unlock()
			break
		}
	}
	return nil
}

// this func should only be called when closed the PandoStore to persist the cache to metaStore
func (ps *PandoStore) persistCache() {
	keys := ps.cache.Keys()
	removeCh := make(chan interface{})
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case k, ok := <-removeCh:
					if !ok {
						return
					}
					ps.cache.Remove(k)
				}
			}
		}()
	}
	for _, k := range keys {
		removeCh <- k
	}
	close(removeCh)

	wg.Wait()
}
