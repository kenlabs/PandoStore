package statestore

import (
	"context"
	"fmt"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dtsync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	storeError "github.com/kenlabs/pando-store/pkg/error"
	"github.com/kenlabs/pando-store/pkg/hamt"
	"github.com/kenlabs/pando-store/pkg/statestore/registry"
	"github.com/kenlabs/pando-store/pkg/types/cbortypes"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

var (
	//providerInfoPrefix  = "/providerInfo"
	//providerStatePrefix = "/providerState"
	rootKey = "/MetaStateStore"
)

var log = logging.Logger("state-store")

type MetaInfo struct {
	provider    peer.ID
	key         cid.Cid
	metaContext []byte
}

type MetaStateStore struct {
	ds             datastore.Batching
	cs             adt.Store
	workingTasksWg sync.WaitGroup
	closeDone      chan struct{}
	pauseWork      chan struct{}
	tasks          []*MetaInfo
	tasksLock      sync.Mutex
	root           hamt.Map
	registry       *registry.Registry
	ctx            context.Context
	cncl           context.CancelFunc
}

func New(ctx context.Context, mds *dtsync.MutexDatastore, as adt.Store) (*MetaStateStore, error) {
	reg, err := registry.New(ctx, mds)
	if err != nil {
		return nil, err
	}
	childCtx, cncl := context.WithCancel(ctx)
	ps := &MetaStateStore{
		ds:        mds,
		cs:        as,
		ctx:       childCtx,
		closeDone: make(chan struct{}),
		pauseWork: make(chan struct{}),
		tasks:     make([]*MetaInfo, 0),
		cncl:      cncl,
		registry:  reg,
	}
	err = ps.init(childCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to init MetaStateStore, err: %v", err)
	}

	go ps.work()
	return ps, nil
}

func (ms *MetaStateStore) init(ctx context.Context) error {
	if ms.ds == nil || ms.cs == nil {
		return fmt.Errorf("nil database")
	}
	root, err := ms.ds.Get(ctx, datastore.NewKey(rootKey))
	if err != nil && err != datastore.ErrNotFound {
		return err
	}

	// find root and load
	if err == nil && root != nil {
		_, rootcid, err := cid.CidFromBytes(root)
		if err != nil {
			return fmt.Errorf("failed to load MetaStateStore root")
		}
		log.Debugf("find root cid %s, loading...", rootcid.String())

		m, err := adt.AsMap(ms.cs, rootcid, builtin.DefaultHamtBitwidth)
		// failed to load hamt root
		if err != nil {
			return fmt.Errorf("failed to load hamt root from cid: %s\r\n%s", rootcid.String(), err.Error())
		}
		// load root successfully
		ms.root = m
		return nil
	}

	// create new hamt
	emptyRoot, err := adt.MakeEmptyMap(ms.cs, builtin.DefaultHamtBitwidth)
	if err != nil {
		return err
	}
	ms.root = emptyRoot
	return nil
}

func (ms *MetaStateStore) AddMetaInfo(provider peer.ID, key cid.Cid, metaContext []byte) error {
	ms.tasksLock.Lock()
	defer ms.tasksLock.Unlock()

	select {
	case _ = <-ms.ctx.Done():
		log.Errorf("store has been closed, new tasks should not be received!")
		return storeError.StoreClosed
	case _ = <-ms.pauseWork:
		log.Errorf("should not receive new task while generating snapshot")
		return storeError.WrongDBState
	default:
	}

	ms.workingTasksWg.Add(1)
	ms.tasks = append(ms.tasks, &MetaInfo{
		provider:    provider,
		key:         key,
		metaContext: metaContext,
	})
	return nil
}

func (ms *MetaStateStore) providerAddMeta(ctx context.Context, provider peer.ID, key cid.Cid, metaContext []byte) error {
	defer ms.workingTasksWg.Done()
	err := ms.registry.UpdateProviderInfo(ctx, provider, key, 0)
	if err != nil {
		return err
	}
	hkey := hamt.StateKey{
		Meta: key,
	}

	exist, err := ms.root.Get(hkey, nil)
	if err != nil {
		return err
	}
	if exist {
		return storeError.KeyHasExisted
	}
	err = ms.root.Put(hkey, &cbortypes.MetaState{
		ProviderID:     provider.String(),
		SnapShotCid:    "",
		SnapShotHeight: 0,
		Context:        metaContext,
	})
	if err != nil {
		return err
	}
	return nil
}

func (ms *MetaStateStore) UpdateMetaState(ctx context.Context, update map[peer.ID][]cid.Cid, ss *cbortypes.SnapShot, scid cid.Cid) (map[peer.ID][]cid.Cid, error) {
	ms.workingTasksWg.Add(1)
	defer ms.workingTasksWg.Done()
	for p, clist := range update {
		for _, c := range clist {
			key := hamt.StateKey{
				Meta: c,
			}
			mstore := new(cbortypes.MetaState)
			ok, err := ms.root.Get(key, mstore)
			if !ok {
				return nil, fmt.Errorf("nil meta state")
			}
			if err != nil {
				return nil, err
			}
			mstore.SnapShotCid = scid.String()
			mstore.SnapShotHeight = ss.Height
			err = ms.root.Put(key, mstore)
			if err != nil {
				return nil, err
			}
		}
		// update last update height for provider
		err := ms.registry.UpdateProviderInfo(ctx, p, cid.Undef, ss.Height)
		if err != nil {
			return nil, err
		}
	}

	return update, nil
}

func (ms *MetaStateStore) MetaStateRoot() (cid.Cid, error) {
	//ms.tasksLock.Lock()
	//defer ms.tasksLock.Unlock()
	close(ms.pauseWork)
	defer func() {
		ms.pauseWork = make(chan struct{})
	}()

	ms.workingTasksWg.Wait()
	root, err := ms.root.Root()
	if err != nil {
		return cid.Undef, err
	}
	err = ms.ds.Put(context.Background(), datastore.NewKey(rootKey), root.Bytes())
	if err != nil {
		log.Errorf("failed to save hamt root in datastore, err:%v", err)
		return cid.Undef, err
	}
	return root, nil
}

func (ms *MetaStateStore) GetMetaInfo(ctx context.Context, c cid.Cid) (*cbortypes.MetaState, error) {
	key := hamt.StateKey{Meta: c}
	state := new(cbortypes.MetaState)
	ok, err := ms.root.Get(key, state)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("meta state about cid: [%s] not existed", c.String())
	}
	return state, nil
}

func (ms *MetaStateStore) GetProviderInfo(ctx context.Context, p peer.ID) (*registry.ProviderInfo, error) {
	info, err := ms.registry.ProviderInfo(ctx, p)
	return info, err
}

func (ms *MetaStateStore) Close() error {
	// close the tasks receiver
	ms.cncl()
	// wait working tasks finished
	<-ms.closeDone
	ms.workingTasksWg.Wait()
	c, err := ms.root.Root()
	if err != nil {
		log.Errorf("failed to flush hamt to store, err: %v", err)
		return err
	}
	err = ms.ds.Put(context.Background(), datastore.NewKey(rootKey), c.Bytes())
	if err != nil {
		log.Errorf("failed to save hamt root in datastore, err:%v", err)
		return err
	}
	return nil
}

func (ms *MetaStateStore) work() {
	for {
		// if cncl() is called and all tasks are done, return and close the signal channel
		select {
		case _ = <-ms.ctx.Done():
			if len(ms.tasks) == 0 {
				close(ms.closeDone)
				return
			}
		default:
		}

		// no new tasks and in working, waiting
		if len(ms.tasks) == 0 {
			time.Sleep(time.Second)
			continue
		}

		ms.tasksLock.Lock()
		task := ms.tasks[0]
		ms.tasks = ms.tasks[1:]
		ms.tasksLock.Unlock()
		err := ms.providerAddMeta(ms.ctx, task.provider, task.key, task.metaContext)
		if err != nil {
			log.Errorf("failed to add metadata for cid: %s , err: %v", task.key.String(), err)
		}
	}
}
