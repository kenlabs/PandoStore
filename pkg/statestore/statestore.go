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
	"github.com/kenlabs/PandoStore/pkg/error"
	"github.com/kenlabs/PandoStore/pkg/hamt"
	"github.com/kenlabs/PandoStore/pkg/statestore/registry"
	"github.com/kenlabs/PandoStore/pkg/types/cbortypes"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

var (
	//providerInfoPrefix  = "/providerInfo"
	//providerStatePrefix = "/providerState"
	rootKey = "/MetaStateStore"
)

var log = logging.Logger("provider-store")

type metaInfo struct {
	provider    peer.ID
	key         cid.Cid
	metaContext []byte
}

type MetaStateStore struct {
	ds             datastore.Batching
	cs             adt.Store
	workingTasksWg sync.WaitGroup
	workingCh      chan struct{}
	queuedTasks    chan *metaInfo
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
		ds:          mds,
		cs:          as,
		ctx:         childCtx,
		queuedTasks: make(chan *metaInfo, 1024*16),
		workingCh:   make(chan struct{}),
		cncl:        cncl,
		registry:    reg,
	}
	err = ps.init(childCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to init MetaStateStore, err: %v", err)
	}

	go ps.work()
	return ps, nil
}

func (ps *MetaStateStore) init(ctx context.Context) error {
	if ps.ds == nil || ps.cs == nil {
		return fmt.Errorf("nil database")
	}
	root, err := ps.ds.Get(ctx, datastore.NewKey(rootKey))
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

		m, err := adt.AsMap(ps.cs, rootcid, builtin.DefaultHamtBitwidth)
		// failed to load hamt root
		if err != nil {
			return fmt.Errorf("failed to load hamt root from cid: %s\r\n%s", rootcid.String(), err.Error())
		}
		// load root successfully
		ps.root = m
		return nil
	}

	// create new hamt
	emptyRoot, err := adt.MakeEmptyMap(ps.cs, builtin.DefaultHamtBitwidth)
	if err != nil {
		return err
	}
	ps.root = emptyRoot
	return nil
}

func (ps *MetaStateStore) AddMetaInfo(ctx context.Context, provider peer.ID, key cid.Cid, metaContext []byte) {
	cctx, cncl := context.WithTimeout(ctx, time.Minute)
	defer cncl()
	select {
	case ps.queuedTasks <- &metaInfo{
		provider:    provider,
		key:         key,
		metaContext: metaContext,
	}:
	case <-cctx.Done():
		log.Errorf("time out or be canceled")
		return
	}
}

func (ps *MetaStateStore) providerAddMeta(ctx context.Context, provider peer.ID, key cid.Cid, metaContext []byte) error {
	ps.workingTasksWg.Add(1)
	defer ps.workingTasksWg.Done()
	err := ps.registry.UpdateProviderInfo(ctx, provider, key, 0)
	if err != nil {
		return err
	}
	hkey := hamt.StateKey{
		Meta: key,
	}

	exist, err := ps.root.Get(hkey, nil)
	if err != nil {
		return err
	}
	if exist {
		return storeError.KeyHasExisted
	}
	err = ps.root.Put(hkey, &cbortypes.MetaState{
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

func (ps *MetaStateStore) ProvidersUpdateMeta(ctx context.Context, update map[peer.ID][]cid.Cid, ss *cbortypes.SnapShot, scid cid.Cid) (map[peer.ID][]cid.Cid, error) {
	ps.workingTasksWg.Add(1)
	defer ps.workingTasksWg.Done()
	for p, clist := range update {
		for idx, c := range clist {
			key := hamt.StateKey{
				Meta: c,
			}
			mstore := new(cbortypes.MetaState)
			ok, err := ps.root.Get(key, mstore)
			if !ok {
				// metaState is in queued, wait
				continue
			}
			if err != nil {
				return nil, err
			}
			mstore.SnapShotCid = scid.String()
			mstore.SnapShotHeight = ss.Height
			err = ps.root.Put(key, mstore)
			if err != nil {
				return nil, err
			}
			update[p] = append(update[p][:idx], update[p][idx+1:]...)
		}
		// update last update height for provider
		err := ps.registry.UpdateProviderInfo(ctx, p, cid.Undef, ss.Height)
		if err != nil {
			return nil, err
		}
	}

	return update, nil
}

func (ps *MetaStateStore) MetaStateRoot() (cid.Cid, error) {
	ps.workingTasksWg.Wait()
	root, err := ps.root.Root()
	if err != nil {
		return cid.Undef, err
	}
	err = ps.ds.Put(context.Background(), datastore.NewKey(rootKey), root.Bytes())
	if err != nil {
		log.Errorf("failed to save hamt root in datastore, err:%v", err)
		return cid.Undef, err
	}
	return root, nil
}

func (ps *MetaStateStore) GetMetaInfo(ctx context.Context, c cid.Cid) (*cbortypes.MetaState, error) {
	key := hamt.StateKey{Meta: c}
	state := new(cbortypes.MetaState)
	ok, err := ps.root.Get(key, state)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("meta state about cid: [%s] not existed", c.String())
	}
	return state, nil
}

func (ps *MetaStateStore) GetProviderInfo(ctx context.Context, p peer.ID) (*registry.ProviderInfo, error) {
	info, err := ps.registry.ProviderInfo(ctx, p)
	return info, err
}

func (ps *MetaStateStore) Close() error {
	close(ps.queuedTasks)
	// wait working tasks finished
	<-ps.workingCh
	ps.cncl()
	ps.workingTasksWg.Wait()
	c, err := ps.root.Root()
	if err != nil {
		log.Errorf("failed to flush hamt to store, err: %v", err)
		return err
	}
	err = ps.ds.Put(context.Background(), datastore.NewKey(rootKey), c.Bytes())
	if err != nil {
		log.Errorf("failed to save hamt root in datastore, err:%v", err)
		return err
	}
	return nil
}

func (ps *MetaStateStore) work() {
	for {
		select {
		case task, ok := <-ps.queuedTasks:
			if !ok {
				log.Warnf("task channel has been closed, quit...")
				close(ps.workingCh)
				return
			}
			err := ps.providerAddMeta(ps.ctx, task.provider, task.key, task.metaContext)
			if err != nil {
				log.Errorf("failed to add metadata for cid: %s , err: %v", task.key.String(), err)
			}
		}
	}
}
