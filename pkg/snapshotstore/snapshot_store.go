package snapshotstore

import (
	"PandoStore/pkg/hamt"
	"PandoStore/pkg/types"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

const (
	SnapShotListKey = "/SnapShotList"
	rootKey         = "/SnapShotStore"
)

var log = logging.Logger("SnapShotStore")

type SnapShotStore struct {
	ds            datastore.Batching
	cs            cbor.IpldStore
	root          hamt.Map
	curSnapShot   cid.Cid
	curHeight     uint64
	snapShotMutex sync.Mutex
}

func NewStore(ctx context.Context, ds datastore.Batching, cs cbor.IpldStore) (*SnapShotStore, error) {
	store := &SnapShotStore{
		ds: ds,
		cs: cs,
	}
	return store, nil
}

func (s *SnapShotStore) init(ctx context.Context) error {
	slist, err := s.loadSnapShotList(ctx)
	if err != nil {
		return err
	}
	if slist != nil {
		ss := new(types.SnapShot)
		newestCid := (*slist)[len(*slist)-1]
		err = s.cs.Get(ctx, newestCid, ss)
		if err != nil {
			return fmt.Errorf("failed to load the newest snapshot: %s", newestCid.String())
		}
		s.curSnapShot = newestCid
		s.curHeight = uint64(len(*slist))
	} else {
		s.curSnapShot = cid.Undef
		s.curHeight = 0
	}

	//load or creat hamt
	//if s.ds == nil || s.cs == nil {
	//	return fmt.Errorf("nil database")
	//}
	//root, err := s.ds.Get(ctx, datastore.NewKey(rootKey))
	//if err != nil {
	//	return err
	//}

	//// find root and load
	//if err == nil && root != nil {
	//	_, rootcid, err := cid.CidFromBytes(root)
	//	if err != nil {
	//		return fmt.Errorf("failed to load ProviderStore root")
	//	}
	//	log.Debugf("find root cid %s, loading...", rootcid.String())
	//
	//	m, err := adt.AsMap(s.cs, rootcid, builtin.DefaultHamtBitwidth)
	//	// failed to load hamt root
	//	if err != nil {
	//		return fmt.Errorf("failed to load hamt root from cid: %s\r\n%s", rootcid.String(), err.Error())
	//	}
	//	// load root successfully
	//	s.root = m
	//	return nil
	//}
	//
	//// create new hamt
	//emptyRoot, err := adt.MakeEmptyMap(s.cs, builtin.DefaultHamtBitwidth)
	//if err != nil {
	//	return err
	//}
	//s.root = emptyRoot
	return nil
}

func (s *SnapShotStore) loadSnapShotList(ctx context.Context) (*[]cid.Cid, error) {
	snapShotList, err := s.ds.Get(ctx, datastore.NewKey(SnapShotListKey))
	if err == nil && snapShotList != nil {
		res := []cid.Cid{}
		err := json.Unmarshal(snapShotList, &res)
		if err != nil {
			return nil, fmt.Errorf("failed to load the snapshot cid list from datastore")
		}
		return &res, nil
	}
	return nil, nil
}

func (s *SnapShotStore) GenerateSnapShot(ctx context.Context, update map[peer.ID][]cid.Cid, stateRoot cid.Cid) (cid.Cid, *types.SnapShot, error) {
	s.snapShotMutex.Lock()
	defer s.snapShotMutex.Unlock()

	if !stateRoot.Defined() {
		return cid.Undef, nil, fmt.Errorf("invalid state root")
	}

	if len(update) == 0 {
		return cid.Undef, nil, fmt.Errorf("update can not be nil")
	}

	var height uint64
	var previousSs string
	if s.curSnapShot == cid.Undef {
		// todo
		height = uint64(0)
		previousSs = ""
	} else {
		height = s.curHeight
		previousSs = s.curSnapShot.String()
	}

	_update := make(map[string]*types.Metalist)
	for p, state := range update {
		_update[p.String()] = &types.Metalist{MetaList: state}
	}
	newSnapShot := &types.SnapShot{
		Update:       _update,
		Height:       height,
		StateRoot:    stateRoot,
		CreateTime:   uint64(time.Now().UnixNano()),
		PrevSnapShot: previousSs,
	}
	c, err := s.cs.Put(ctx, newSnapShot)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("falied to save the snapshot. %v", err)
	}
	s.curSnapShot = c
	s.curHeight++
	err = s.updateSnapShotCidList(ctx, c)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("error happened while updating the cidlist of snapshot.%v", err)
	}
	return c, newSnapShot, nil
}

func (s *SnapShotStore) updateSnapShotCidList(ctx context.Context, newSsCid cid.Cid) error {
	snapShotList, err := s.ds.Get(ctx, datastore.NewKey(SnapShotListKey))
	if err == nil && snapShotList != nil {
		var ssCidList []cid.Cid
		err := json.Unmarshal(snapShotList, &ssCidList)
		if err != nil {
			return fmt.Errorf("failed to load the snapshot cid list from datastore")
		}

		ssCidList = append(ssCidList, newSsCid)
		ssCidListBytes, err := json.Marshal(ssCidList)
		if err != nil {
			return fmt.Errorf("failed to marshal the cidlist of snapshot. %s", err.Error())
		}
		err = s.ds.Put(ctx, datastore.NewKey(SnapShotListKey), ssCidListBytes)
		if err != nil {
			return fmt.Errorf("failed to save the new snap shot cid list in ds")
		}
	} else {
		ssCidList := []cid.Cid{newSsCid}
		ssCidListBytes, err := json.Marshal(ssCidList)
		if err != nil {
			return fmt.Errorf("failed to marshal the cidlist of snapshot. %s", err.Error())
		}
		err = s.ds.Put(ctx, datastore.NewKey(SnapShotListKey), ssCidListBytes)
		if err != nil {
			return fmt.Errorf("failed to save the new snap shot cid list in ds")
		}
	}
	return nil
}
