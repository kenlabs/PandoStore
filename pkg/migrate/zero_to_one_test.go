package migrate_test

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	dtsync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/kenlabs/pando-store/pkg"
	"github.com/kenlabs/pando-store/pkg/config"
	"github.com/kenlabs/pando-store/pkg/metastore"
	"github.com/kenlabs/pando-store/pkg/migrate"
	"github.com/kenlabs/pando-store/pkg/store"
	"github.com/stretchr/testify/assert"

	//"github.com/kenlabs/PandoStore/pkg/store"
	"github.com/kenlabs/pando-store/pkg/types/cbortypes"
	"path"
	"testing"
)

var (
	testdata1 = []byte("testdata1")
	testdata2 = []byte("testdata2")
	cid1      cid.Cid
	cid2      cid.Cid
)

func init() {
	u.Debug = true
	var err error
	cid1, err = cbortypes.LinkProto.Sum(testdata1)
	if err != nil {
		panic(err)
	}
	cid2, err = cbortypes.LinkProto.Sum(testdata2)
	if err != nil {
		panic(err)
	}
}

func genTestOldStore(t *testing.T, path string) error {
	db, err := leveldb.NewDatastore(path, nil)
	if err != nil {
		return err
	}
	mds := dtsync.MutexWrap(db)
	bs := blockstore.NewBlockstore(mds)

	block1, err := blocks.NewBlockWithCid(testdata1, cid1)
	if err != nil {
		return err
	}
	block2, err := blocks.NewBlockWithCid(testdata2, cid2)
	if err != nil {
		return err
	}
	err = bs.Put(context.Background(), block1)
	if err != nil {
		return err
	}
	err = bs.Put(context.Background(), block2)
	if err != nil {
		return err
	}

	err = mds.Put(context.Background(), datastore.NewKey("/sync/12D3KooWSS3sEujyAXB9SWUvVtQZmxH6vTi9NitqaaRQoUjeEk3M"), cid2.Bytes())
	if err != nil {
		return err
	}
	err = mds.Put(context.Background(), datastore.NewKey("/sync/12D3KooWSS3sEujyAXB9SWUvVtQZmxH6vTi9NitqaaRQoUjeEk4M"), cid1.Bytes())
	if err != nil {
		return err
	}

	return mds.Close()
}

func TestReadMigrate(t *testing.T) {
	tmpDir := t.TempDir()
	oldStoreDir := path.Join(tmpDir, "datastore")
	err := genTestOldStore(t, oldStoreDir)
	if err != nil {
		t.Fatal(err.Error())
	}
	ver, err := pkg.CheckVersion(oldStoreDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(ver)
	ds, err := leveldb.NewDatastore(oldStoreDir, nil)
	if err != nil {
		t.Error(err.Error())
	}

	mds := dtsync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)
	ch, err := bs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatal(err.Error())
	}

	q := query.Query{
		Prefix: "/sync/",
	}
	qres, err := mds.Query(context.Background(), q)

	if err != nil {
		t.Fatal(err.Error())
	}

	for result := range qres.Next() {
		if result.Error != nil {
			t.Fatal(result.Error)
		}
		ent := result.Entry
		_, c, err := cid.CidFromBytes(ent.Value)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("cid version: %d, %s : %s", c.Version(), ent.Key, c.String())
	}

Flag:
	for {
		select {
		case c, exist := <-ch:
			if !exist {
				break Flag
			}
			t.Log(c.String())
			block, err := bs.Get(context.Background(), c)
			if err != nil {
				t.Fatal(err.Error())
			}
			fmt.Printf("%s\r\n", block.RawData())
		}
	}
	tc, err := cid.Decode("baguqeeqqqcif36qis22pak3f476d64se4i")
	if err != nil {
		t.Fatal(err.Error())
	}
	bk, err := bs.Get(context.Background(), tc)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(string(bk.RawData()))
}

func TestMigrate(t *testing.T) {
	tmpDir := t.TempDir()
	oldStoreDir := path.Join(tmpDir, "datastore")
	err := genTestOldStore(t, oldStoreDir)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = migrate.Migrate(pkg.Version0, pkg.Version1, oldStoreDir, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	pdb, err := store.NewStoreFromConfig(context.Background(), &config.StoreConfig{
		Type:             "levelds",
		StoreRoot:        tmpDir,
		Dir:              "datastore",
		SnapShotInterval: "1s",
		CacheSize:        config.DefaultCacheSize,
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	//cc1, _ := cid.Decode("bafkreeeasbo7ucewwtycwzph7q7xerhc")
	//cc2, _ := cid.Decode("bafkreegrinl6sbzptamsocyba7ixjnwdb")

	data1, err := pdb.Get(context.Background(), cid1)
	if err != nil {
		t.Fatal(err.Error())
	}
	data2, err := pdb.Get(context.Background(), cid2)
	if err != nil {
		t.Fatal(err.Error())
	}
	//assert.Equal(t, data1, testdata1)
	//assert.Equal(t, data2, testdata2)
	t.Log(string(data1))
	t.Log(string(data2))

	v, err := pkg.CheckVersion(oldStoreDir)
	assert.NoError(t, err)
	assert.Equal(t, v, pkg.Version1)
}

func TestBlockMigrate(t *testing.T) {
	t.Log(cid1.String())
	ds := datastore.NewMapDatastore()
	mds := dtsync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)

	ds2 := datastore.NewMapDatastore()
	mds2 := dtsync.MutexWrap(ds2)
	//bs2 := blockstore.NewBlockstore(mds2)
	metads := dsns.Wrap(ds2, metastore.MetaPrefix)

	bk, err := blocks.NewBlockWithCid(testdata1, cid1)
	if err != nil {
		t.Fatal(err)
	}
	err = bs.Put(context.Background(), bk)
	if err != nil {
		t.Fatal(err)
	}

	err = mds.Put(context.Background(), datastore.NewKey("/sync/12D3KooWSS3sEujyAXB9SWUvVtQZmxH6vTi9NitqaaRQoUjeEk4M"), cid1.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	ch, err := bs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatal(err.Error())
	}

Flag:
	for {
		select {
		case c, exist := <-ch:
			if !exist {
				break Flag
			}
			t.Log(c.String())
			block, err := bs.Get(context.Background(), c)
			if err != nil {
				t.Fatal(err.Error())
			}
			t.Log(string(block.RawData()))

			err = metads.Put(context.Background(), dshelp.MultihashToDsKey(c.Hash()), block.RawData())
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	err = metads.Close()
	if err != nil {
		t.Fatal(err)
	}
	newMetads := dsns.Wrap(mds2, metastore.MetaPrefix)
	res, err := newMetads.Get(context.Background(), dshelp.MultihashToDsKey(cid1.Hash()))
	t.Log(string(res))

}

func TestGetNodeByDifCid(t *testing.T) {
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(ds)

	bk, err := blocks.NewBlockWithCid(testdata1, cid1)
	if err != nil {
		t.Fatal(err)
	}

	err = bs.Put(context.Background(), bk)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("cid1: ", cid1.String())
	res, err := ds.Query(context.Background(), query.Query{
		Prefix: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	for r := range res.Next() {
		t.Log(r.Entry.Key)
	}

	exist, err := bs.Has(context.Background(), cid1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(dshelp.MultihashToDsKey(cid1.Hash()))
	assert.Equal(t, exist, true)

	ch, err := bs.AllKeysChan(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	//bs.Get()
	for c := range ch {
		t.Log(c.String())
		t.Log(dshelp.MultihashToDsKey(c.Hash()))
	}

}
