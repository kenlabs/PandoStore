package migrate

import (
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dtsync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/kenlabs/PandoStore/pkg"
	"github.com/kenlabs/PandoStore/pkg/types/cbortypes"
	"path"
	"testing"
)

func genTestOldStore(t *testing.T, path string) error {
	u.Debug = true
	db, err := leveldb.NewDatastore(path, nil)
	if err != nil {
		return err
	}
	mds := dtsync.MutexWrap(db)
	bs := blockstore.NewBlockstore(mds)

	testdata1 := []byte("testdata1")
	testdata2 := []byte("testdata2")
	cid1, err := cbortypes.LinkProto.Sum(testdata1)
	if err != nil {
		return err
	}
	cid2, err := cbortypes.LinkProto.Sum(testdata2)
	if err != nil {
		return err
	}
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

	b, err := bs.Get(context.Background(), cid2)
	if err != nil {
		return err
	}
	t.Log(string(b.RawData()))
	t.Log(b.String())

	return mds.Close()
}

func TestReadMigrate(t *testing.T) {
	tmpDir := t.TempDir()
	oldStoreDir := path.Join(tmpDir, "datastore")
	err := genTestOldStore(t, oldStoreDir)
	if err != nil {
		t.Fatal(err.Error())
	}
	//storeDir := "/Users/zxh/.pando/_datastore"
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
	err = Migrate(pkg.Version0, pkg.Version1, oldStoreDir, false)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestBlockMigrate(t *testing.T) {
	testdata1 := []byte("testdata1")
	cid1, err := cbortypes.LinkProto.Sum(testdata1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(cid1.String())
	ds := datastore.NewMapDatastore()
	mds := dtsync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)

	ds2 := datastore.NewMapDatastore()
	mds2 := dtsync.MutexWrap(ds2)
	bs2 := blockstore.NewBlockstore(mds2)

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

			err = bs2.Put(context.Background(), block)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	res, err := bs2.Get(context.Background(), cid1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(res.RawData()))

}
