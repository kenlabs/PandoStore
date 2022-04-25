package store

import (
	"PandoStore/pkg/config"
	"PandoStore/pkg/metastore"
	"PandoStore/pkg/snapshotstore"
	"PandoStore/pkg/statestore"
	"context"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"

	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	lp = cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}

	cid1, _  = lp.Sum([]byte("testdata1"))
	cid2, _  = lp.Sum([]byte("testdata2"))
	cid3, _  = lp.Sum([]byte("testdata3"))
	peer1, _ = peer.Decode("12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ")
)

func TestRoundTripPandoStore(t *testing.T) {
	_ = logging.SetLogLevel("PandoStore", "debug")

	ctx := context.Background()
	ds := datastore.NewMapDatastore()
	mds := sync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)
	cs := cbor.NewCborStore(bs)
	adtstore := adt.WrapStore(ctx, cs)

	ms, err := metastore.New(mds)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ps, err := statestore.New(ctx, mds, adtstore)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ss, err := snapshotstore.NewStore(ctx, mds, cs)
	if err != nil {
		t.Fatalf(err.Error())
	}
	cfg := &config.StoreConfig{SnapShotInterval: time.Second.String()}
	store, err := NewPandoStore(ctx, ms, ps, ss, cfg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = store.Store(ctx, cid1.String(), []byte("testdata1"), peer1, nil)
	if err != nil {
		t.Fatal(err.Error())
	}

	time.Sleep(time.Second * 2)
	info, err := store.MetaInclusion(ctx, cid1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	t.Logf("%#v", info)

	snapshot, err := store.SnapShotStore.GetSnapShotByHeight(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", snapshot)

	err = store.Store(ctx, cid1.String(), []byte("testdata1"), peer1, nil)
	assert.Contains(t, err.Error(), "key has existed")

	snapshot, err = store.SnapShotStore.GetSnapShotByCid(ctx, cid3)
	assert.Contains(t, err.Error(), "not found")

	pinfo, err := store.metaStateStore.GetProviderInfo(ctx, peer1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%v", pinfo)

	//time.Sleep(time.Second * 999)
}

func TestRestartPandoStore(t *testing.T) {
	_ = logging.SetLogLevel("PandoStore", "debug")
	ctx := context.Background()
	testdir := t.TempDir()
	cfg := &config.StoreConfig{
		Type:             "levelds",
		StoreRoot:        "",
		Dir:              testdir,
		SnapShotInterval: "1s",
	}
	db, err := LoadStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	//db, err = LoadStoreFromConfig(ctx, cfg)
	//if err != nil {
	//	t.Fatal(err)
	//}
}
