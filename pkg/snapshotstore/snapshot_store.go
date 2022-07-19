package snapshotstore

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dtsync "github.com/ipfs/go-datastore/sync"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	storeError "github.com/kenlabs/pando-store/pkg/error"
	"github.com/kenlabs/pando-store/pkg/hamt"
	"github.com/kenlabs/pando-store/pkg/types/cbortypes"
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
	mds           *dtsync.MutexDatastore
	cs            cbor.IpldStore
	root          hamt.Map
	curSnapShot   cid.Cid
	curHeight     uint64
	snapShotMutex sync.Mutex
}

func NewStore(ctx context.Context, mds *dtsync.MutexDatastore, cs cbor.IpldStore) (*SnapShotStore, error) {
	store := &SnapShotStore{
		mds: mds,
		cs:  cs,
	}
	return store, nil
}

func (s *SnapShotStore) init(ctx context.Context) error {
	slist, err := s.GetSnapShotList(ctx)
	if err != nil {
		return err
	}
	if slist != nil {
		ss := new(cbortypes.SnapShot)
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

	return nil
}

func (s *SnapShotStore) GetSnapShotList(ctx context.Context) (*[]cid.Cid, error) {
	snapShotList, err := s.mds.Get(ctx, datastore.NewKey(SnapShotListKey))
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

func (s *SnapShotStore) GenerateSnapShot(ctx context.Context, update map[peer.ID][]cid.Cid, stateRoot cid.Cid) (cid.Cid, *cbortypes.SnapShot, error) {
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

	_update := make(map[string]*cbortypes.Metalist)
	for p, state := range update {
		_update[p.String()] = &cbortypes.Metalist{MetaList: state}
	}
	newSnapShot := &cbortypes.SnapShot{
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
	snapShotList, err := s.mds.Get(ctx, datastore.NewKey(SnapShotListKey))
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
		err = s.mds.Put(ctx, datastore.NewKey(SnapShotListKey), ssCidListBytes)
		if err != nil {
			return fmt.Errorf("failed to save the new snap shot cid list in mds")
		}
	} else {
		ssCidList := []cid.Cid{newSsCid}
		ssCidListBytes, err := json.Marshal(ssCidList)
		if err != nil {
			return fmt.Errorf("failed to marshal the cidlist of snapshot. %s", err.Error())
		}
		err = s.mds.Put(ctx, datastore.NewKey(SnapShotListKey), ssCidListBytes)
		if err != nil {
			return fmt.Errorf("failed to save the new snap shot cid list in mds")
		}
	}
	return nil
}

func (s *SnapShotStore) GetSnapShotByCid(ctx context.Context, c cid.Cid) (*cbortypes.SnapShot, error) {
	if !c.Defined() {
		return nil, fmt.Errorf("invalid cid")
	}
	res := new(cbortypes.SnapShot)
	err := s.cs.Get(ctx, c, res)
	if err != nil {
		log.Errorf("failed to get SnapShot from store, err :%v", err)
		return nil, err
	}
	return res, nil
}

func (s *SnapShotStore) GetSnapShotByHeight(ctx context.Context, h uint64) (*cbortypes.SnapShot, cid.Cid, error) {
	slist, err := s.GetSnapShotList(ctx)
	if err != nil {
		return nil, cid.Undef, err
	}
	if slist == nil || len(*slist) == 0 || h+1 > uint64(len(*slist)) {
		log.Errorf("invalid height: %d", h)
		return nil, cid.Undef, storeError.InvalidParameters
	}
	c := (*slist)[h]
	snapshot, err := s.GetSnapShotByCid(ctx, c)
	if err != nil {
		return nil, cid.Undef, err
	}
	return snapshot, c, nil
}

func (s *SnapShotStore) Close() error {
	s.snapShotMutex.Lock()
	defer s.snapShotMutex.Unlock()
	return nil
}
