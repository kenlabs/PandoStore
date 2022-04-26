package hamt

import (
	"context"
	"github.com/kenlabs/PandoStore/pkg/types/cbortypes"
	//"github.com/kenlabs/pando/pkg/statetree/types"
	"github.com/stretchr/testify/assert"
	//"github.com/kenlabs/pando/statetree/hamt"

	"fmt"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	"testing"
)

func TestMapSaveAndLoad(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewMapDatastore()
	mds := dssync.MutexWrap(ds)
	bs := blockstore.NewBlockstore(mds)
	cs := cbor.NewCborStore(bs)
	store := adt.WrapStore(context.TODO(), cs)
	emptyRoot, err := adt.MakeEmptyMap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		t.Error(err)
	}
	state1 := new(cbortypes.MetaState)
	state2 := new(cbortypes.MetaState)

	testCid1, _ := cid.Decode("bafy2bzaceamp42wmmgr2g2ymg46euououzfyck7szknvfacqscohrvaikwfaa")
	testCid2, _ := cid.Decode("bafy2bzaceamp42wmmgr2g2ymg46euououzfyck7szknvfacqscohrvaikwfab")
	//testCid3, _ := cid.Decode("bafy2bzaceamp42wmmgr2g2ymg46euououzfyck7szknvfacqscohrvaikwfac")

	//state1.MetaList = append(state1.MetaList, testCid1, testCid2)
	//state2.MetaList = append(state2.MetaList, testCid3)

	err = emptyRoot.Put(StateKey{Meta: testCid1}, state1)
	assert.NoError(t, err)

	err = emptyRoot.Put(StateKey{Meta: testCid2}, state2)
	assert.NoError(t, err)

	newRootCid, err := emptyRoot.Root()
	assert.NoError(t, err)

	fmt.Println(newRootCid.String())
	err = ds.Put(ctx, datastore.NewKey("TESTROOTKEY"), newRootCid.Bytes())
	assert.NoError(t, err)

	rootCidBytest, err := ds.Get(ctx, datastore.NewKey("TESTROOTKEY"))
	assert.NoError(t, err)

	_, rootcid, err := cid.CidFromBytes(rootCidBytest)
	assert.NoError(t, err)

	fmt.Println(rootcid.String())

	_, err = adt.AsMap(store, rootcid, builtin.DefaultHamtBitwidth)
	assert.NoError(t, err)

}
