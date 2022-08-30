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
	"github.com/kenlabs/pando-store/pkg/types/store"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

const (
	SnapShotListKey = "/SnapShotList"
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
	sstore := &SnapShotStore{
		mds: mds,
		cs:  cs,
	}
	err := sstore.init(ctx)
	if err != nil {
		return nil, err
	}
	return sstore, nil
}

func (s *SnapShotStore) init(ctx context.Context) error {
	slist, err := s.GetSnapShotList(ctx)
	if err != nil {
		return err
	}
	if slist != nil && slist.Length != 0 {
		ss := new(cbortypes.SnapShot)
		newestCid := slist.List[slist.Length-1].SnapShotCid
		err = s.cs.Get(ctx, newestCid, ss)
		if err != nil {
			return fmt.Errorf("failed to load the newest snapshot: %s", newestCid.String())
		}
		s.curSnapShot = newestCid
		s.curHeight = uint64(slist.Length)
	} else {
		s.curSnapShot = cid.Undef
		s.curHeight = 0
	}

	return nil
}

func (s *SnapShotStore) GetSnapShotList(ctx context.Context) (*store.SnapShotList, error) {
	snapShotList, err := s.mds.Get(ctx, datastore.NewKey(SnapShotListKey))
	if err == nil && snapShotList != nil {
		res := store.SnapShotList{}
		err := json.Unmarshal(snapShotList, &res)
		if err != nil {
			oldRes := []cid.Cid{}
			err = json.Unmarshal(snapShotList, &oldRes)
			// try migrating the old snapshot cidlist to new snapshotList
			if err == nil {
				newSsList := &store.SnapShotList{}
				newSsList.Length = len(oldRes)
				// migrate
				for idx, c := range oldRes {
					ss, err := s.GetSnapShotByCid(ctx, c)
					if err != nil {
						return nil, fmt.Errorf("failed to migrate the old snapshot list to new struct, err: %s", err.Error())
					}
					if ss.Height != uint64(idx) {
						return nil, fmt.Errorf("mismatched height found in snapshot while migrating the old snapshot list, found: %d, expected: %d", ss.Height, idx)
					}
					newSsList.List = append(newSsList.List, struct {
						CreatedTime uint64
						SnapShotCid cid.Cid
					}{CreatedTime: ss.CreateTime, SnapShotCid: c})
				}
				// save migrated result
				newSnapShotListBytes, err := json.Marshal(newSsList)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal the migrated snapshot list. %s", err.Error())
				}
				err = s.mds.Put(ctx, datastore.NewKey(SnapShotListKey), newSnapShotListBytes)
				if err != nil {
					return nil, fmt.Errorf("failed to save the new snapshotList in mds, err: %s", err)
				}
				return newSsList, nil
			}
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
	err = s.updateSnapShotCidList(ctx, c, newSnapShot.CreateTime)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("error happened while updating the cidlist of snapshot.%v", err)
	}
	return c, newSnapShot, nil
}

func (s *SnapShotStore) updateSnapShotCidList(ctx context.Context, newSsCid cid.Cid, createdTimestamp uint64) error {
	snapShotListBytes, err := s.mds.Get(ctx, datastore.NewKey(SnapShotListKey))
	if err == nil && snapShotListBytes != nil {
		var snapshotList store.SnapShotList
		err := json.Unmarshal(snapShotListBytes, &snapshotList)
		if err != nil {
			return fmt.Errorf("failed to load the snapshot cid list from datastore")
		}

		snapshotList.List = append(snapshotList.List, struct {
			CreatedTime uint64
			SnapShotCid cid.Cid
		}{CreatedTime: createdTimestamp, SnapShotCid: newSsCid})
		snapshotList.Length++

		newSnapShotListBytes, err := json.Marshal(snapshotList)
		if err != nil {
			return fmt.Errorf("failed to marshal the cidlist of snapshot. %s", err.Error())
		}
		err = s.mds.Put(ctx, datastore.NewKey(SnapShotListKey), newSnapShotListBytes)
		if err != nil {
			return fmt.Errorf("failed to save the new snap shot cid list in mds")
		}
	} else {
		snapshotList := store.SnapShotList{}
		snapshotList.List = append(snapshotList.List, struct {
			CreatedTime uint64
			SnapShotCid cid.Cid
		}{CreatedTime: createdTimestamp, SnapShotCid: newSsCid})
		snapshotList.Length = 1
		ssCidListBytes, err := json.Marshal(snapshotList)
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
	if slist == nil || slist.Length == 0 || h+1 > uint64(slist.Length) {
		log.Errorf("invalid height: %d", h)
		return nil, cid.Undef, storeError.InvalidParameters
	}
	c := slist.List[h].SnapShotCid
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
