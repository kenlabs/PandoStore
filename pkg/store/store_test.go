package store

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dtsync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/kenlabs/PandoStore/pkg/config"
	storeError "github.com/kenlabs/PandoStore/pkg/error"
	"github.com/kenlabs/PandoStore/pkg/types/cbortypes"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"path"
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

	testdata1 = []byte("testdata1")
	testdata2 = []byte("testdata2")

	cid1, _ = lp.Sum(testdata1)
	cid2, _ = lp.Sum(testdata2)
	//cid3, _  = lp.Sum([]byte("testdata3"))
	peer1, _ = peer.Decode("12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ")
	peer2, _ = peer.Decode("12D3KooWNU48MUrPEoYh77k99RbskgftfmSm3CdkonijcM5VehS9")
	peer3, _ = peer.Decode("12D3KooWNnK4gnNKmh6JUzRb34RqNcBahN5B8v18DsMxQ8mCqw81")
	peers    = []peer.ID{peer1, peer2, peer3}
)

func TestRoundTripPandoStore(t *testing.T) {
	_ = logging.SetLogLevel("PandoStore", "debug")

	ctx := context.Background()
	ds := datastore.NewMapDatastore()
	mds := dtsync.MutexWrap(ds)

	cfg := &config.StoreConfig{
		SnapShotInterval: time.Second.String(),
		CacheSize:        config.DefaultCacheSize,
	}
	store, err := NewStoreFromDatastore(ctx, mds, cfg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = store.Store(ctx, cid1, []byte("testdata1"), peer1, nil)
	if err != nil {
		t.Fatal(err.Error())
	}

	time.Sleep(time.Second * 2)
	info, err := store.MetaInclusion(ctx, cid1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	t.Logf("%#v", info)

	snapshot, _, err := store.SnapShotStore().GetSnapShotByHeight(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", snapshot)

	err = store.Store(ctx, cid1, []byte("testdata1"), peer1, nil)
	assert.Contains(t, err.Error(), "key has existed")

	snapshot, err = store.SnapShotStore().GetSnapShotByCid(ctx, cid2)
	assert.Contains(t, err.Error(), "not found")

	pinfo, err := store.StateStore.GetProviderInfo(ctx, peer1)
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
	testStoreDir := path.Join(testdir, "teststore1")
	cfg := &config.StoreConfig{
		Type:             "levelds",
		StoreRoot:        "",
		Dir:              testStoreDir,
		SnapShotInterval: "1s",
		CacheSize:        config.DefaultCacheSize,
	}
	db, err := NewStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Store(ctx, cid1, []byte("testdata1"), peer1, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	info, err := db.MetaInclusion(ctx, cid1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 2000)
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	assert.Equal(t, err, storeError.StoreClosed)

	db, err = NewStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	val, err := db.Get(ctx, cid1)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, val, testdata1)
	info, err = db.MetaInclusion(ctx, cid1)
	if err != nil {
		t.Fatal(err)
	}
	_, c, err := db.SnapShotStore().GetSnapShotByHeight(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, info.ID, cid1)
	assert.Equal(t, info.InPando, true)
	assert.Equal(t, info.InSnapShot, true)
	assert.Equal(t, info.SnapShotID, c)
	assert.Equal(t, info.Context, []byte(nil))
	assert.Equal(t, info.Provider, peer1)
	assert.Equal(t, info.SnapShotHeight, uint64(0))
	err = db.Close()
	assert.NoError(t, err)

}

func Test1KWriteWithClose(t *testing.T) {
	_ = logging.SetLogLevel("PandoStore", "debug")
	ctx := context.Background()
	testdir := t.TempDir()
	testStoreDir := path.Join(testdir, "teststore3")
	cfg := &config.StoreConfig{
		Type:             "levelds",
		StoreRoot:        "",
		Dir:              testStoreDir,
		SnapShotInterval: "1s",
		CacheSize:        config.DefaultCacheSize,
	}
	store, err := NewStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]cid.Cid, 0)
	for i := 0; i < 1000; i++ {
		data := make([]byte, rand.Int31n(100))
		rand.Read(data)
		key, _ := cbortypes.LinkProto.Sum(data)
		keys = append(keys, key)
		provid := peers[rand.Intn(len(peers))]
		err = store.Store(ctx, key, data, provid, nil)
		if err != nil {
			if assert.Contains(t, err.Error(), "key has existed") {
				continue
			}
			t.Fatal(err)
		}

	}

	_, err = store.MetaInclusion(ctx, keys[len(keys)-1])
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(keys)/10; i++ {
		_, err = store.Get(ctx, keys[rand.Intn(len(keys))])
		if err != nil {
			t.Fatal(err)
		}
	}

	assert.NoError(t, err)
	err = store.Close()
	assert.NoError(t, err)
	err = store.Close()
	assert.Equal(t, err, storeError.StoreClosed)
}

func Test100KWritePandoStore(t *testing.T) {
	_ = logging.SetLogLevel("PandoStore", "debug")
	_ = logging.SetLogLevel("state-store", "debug")
	ctx := context.Background()
	testdir := t.TempDir()
	testStoreDir := path.Join(testdir, "teststore2")
	cfg := &config.StoreConfig{
		Type:             "levelds",
		StoreRoot:        "",
		Dir:              testStoreDir,
		SnapShotInterval: "1s",
		CacheSize:        config.DefaultCacheSize * 100,
	}
	store, err := NewStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]cid.Cid, 0)
	for i := 0; i < 100000; i++ {
		data := make([]byte, rand.Int31n(1000))
		rand.Read(data)
		key, _ := cbortypes.LinkProto.Sum(data)
		keys = append(keys, key)
		provid := peers[rand.Intn(len(peers))]
		err = store.Store(ctx, key, data, provid, nil)
		if err != nil {
			if assert.Contains(t, err.Error(), "key has existed") {
				continue
			}
			t.Fatal(err)
		}

	}

	_, err = store.MetaInclusion(ctx, keys[len(keys)-1])
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(keys)/10; i++ {
		_, err = store.Get(ctx, keys[rand.Intn(len(keys))])
		if err != nil {
			t.Fatal(err)
		}
	}

	//assert.NoError(t, err)
	//err = store.Close()
	//assert.NoError(t, err)
	//err = store.Close()
	//assert.Equal(t, err, storeError.StoreClosed)
}
