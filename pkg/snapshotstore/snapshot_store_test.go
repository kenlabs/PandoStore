package snapshotstore

import (
	"context"
	"encoding/json"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dtsync "github.com/ipfs/go-datastore/sync"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/kenlabs/pando-store/pkg/types/cbortypes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func genInvalidOldSnapShotListInfo(t *testing.T) (*dtsync.MutexDatastore, cbor.IpldStore) {
	ds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		t.Fatal(err)
	}
	mds := dtsync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)
	cs := cbor.NewCborStore(bs)

	var ssList []cid.Cid
	c1, _ := cbortypes.LinkProto.Sum([]byte("test1"))
	c2, _ := cbortypes.LinkProto.Sum([]byte("test2"))
	ssList = append(ssList, c1)
	ssList = append(ssList, c2)
	ssListBytes, err := json.Marshal(ssList)
	if err != nil {
		t.Fatal(err)
	}
	err = mds.Put(context.Background(), datastore.NewKey(SnapShotListKey), ssListBytes)
	if err != nil {
		t.Fatal(err)
	}

	return mds, cs
}

func genWrongOldSnapShotListInfo(t *testing.T) (*dtsync.MutexDatastore, cbor.IpldStore) {
	ctx := context.Background()
	ds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		t.Fatal(err)
	}
	mds := dtsync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)
	cs := cbor.NewCborStore(bs)

	state1, _ := cbortypes.LinkProto.Sum([]byte("teststateroot1"))
	state2, _ := cbortypes.LinkProto.Sum([]byte("teststateroot2"))
	ss1 := &cbortypes.SnapShot{
		Update:       nil,
		StateRoot:    state1,
		Height:       0,
		CreateTime:   uint64(time.Now().UnixNano()),
		PrevSnapShot: "",
	}
	c1, err := cs.Put(ctx, ss1)
	if err != nil {
		t.Fatal(err)
	}
	ss2 := &cbortypes.SnapShot{
		Update:       nil,
		StateRoot:    state2,
		Height:       0,
		CreateTime:   0,
		PrevSnapShot: c1.String(),
	}
	c2, err := cs.Put(ctx, ss2)

	var ssList []cid.Cid
	ssList = append(ssList, c1)
	ssList = append(ssList, c2)
	ssListBytes, err := json.Marshal(ssList)
	if err != nil {
		t.Fatal(err)
	}
	err = mds.Put(context.Background(), datastore.NewKey(SnapShotListKey), ssListBytes)
	if err != nil {
		t.Fatal(err)
	}

	return mds, cs
}

func genValidSnapShotListInfo(t *testing.T) (*dtsync.MutexDatastore, cbor.IpldStore) {
	ctx := context.Background()
	ds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		t.Fatal(err)
	}
	mds := dtsync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)
	cs := cbor.NewCborStore(bs)

	state1, _ := cbortypes.LinkProto.Sum([]byte("teststateroot1"))
	state2, _ := cbortypes.LinkProto.Sum([]byte("teststateroot2"))
	state3, _ := cbortypes.LinkProto.Sum([]byte("teststateroot3"))
	ss1 := &cbortypes.SnapShot{
		Update:       nil,
		StateRoot:    state1,
		Height:       0,
		CreateTime:   uint64(time.Now().UnixNano()),
		PrevSnapShot: "",
	}
	c1, err := cs.Put(ctx, ss1)
	if err != nil {
		t.Fatal(err)
	}
	ss2 := &cbortypes.SnapShot{
		Update:       nil,
		StateRoot:    state2,
		Height:       1,
		CreateTime:   uint64(time.Now().UnixNano()),
		PrevSnapShot: c1.String(),
	}
	c2, err := cs.Put(ctx, ss2)
	if err != nil {
		t.Fatal(err)
	}

	ss3 := &cbortypes.SnapShot{
		Update:       nil,
		StateRoot:    state3,
		Height:       2,
		CreateTime:   uint64(time.Now().UnixNano()),
		PrevSnapShot: c2.String(),
	}
	c3, err := cs.Put(ctx, ss3)
	if err != nil {
		t.Fatal(err)
	}

	var ssList []cid.Cid
	ssList = append(ssList, c1)
	ssList = append(ssList, c2)
	ssList = append(ssList, c3)
	ssListBytes, err := json.Marshal(ssList)
	if err != nil {
		t.Fatal(err)
	}
	err = mds.Put(context.Background(), datastore.NewKey(SnapShotListKey), ssListBytes)
	if err != nil {
		t.Fatal(err)
	}

	return mds, cs
}

func TestMigrateOldSnapShotList(t *testing.T) {
	ctx := context.Background()
	mds, cs := genInvalidOldSnapShotListInfo(t)
	sstore, err := NewStore(ctx, mds, cs)
	if err == nil || sstore != nil {
		t.Fatal("unexpected result")
	}
	assert.Contains(t, err.Error(), "failed to migrate the old snapshot list to new struct")
	assert.Contains(t, err.Error(), "not found")

	mds, cs = genWrongOldSnapShotListInfo(t)
	sstore, err = NewStore(ctx, mds, cs)
	if err == nil || sstore != nil {
		t.Fatal("unexpected result")
	}
	assert.Contains(t, err.Error(), "mismatched height found in snapshot while migrating the old snapshot list")

	mds, cs = genValidSnapShotListInfo(t)
	sstore, err = NewStore(ctx, mds, cs)
	assert.NoError(t, err)
	assert.Equal(t, sstore.curHeight, uint64(3))
	err = sstore.Close()
	assert.NoError(t, err)
	sstore, err = NewStore(ctx, mds, cs)
	assert.Equal(t, sstore.curHeight, uint64(3))

}
