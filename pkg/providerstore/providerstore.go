package providerstore

import (
	"PandoStore/pkg/hamt"
	"PandoStore/pkg/providerstore/registry"
	"PandoStore/pkg/providerstore/types"
	"context"
	"fmt"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

var (
	//providerInfoPrefix  = "/providerInfo"
	//providerStatePrefix = "/providerState"
	rootKey = "ProviderStore"
)

var log = logging.Logger("provider-store")

type ProviderStore struct {
	ds       datastore.Batching
	cs       adt.Store
	root     hamt.Map
	registry *registry.Registry
	ctx      context.Context
	cncl     context.CancelFunc
}

func New(ctx context.Context, ds datastore.Batching, cs adt.Store) (*ProviderStore, error) {
	reg, err := registry.New(ctx, ds)
	if err != nil {
		return nil, err
	}
	childCtx, cncl := context.WithCancel(ctx)
	ps := &ProviderStore{
		ds:       ds,
		cs:       cs,
		ctx:      childCtx,
		cncl:     cncl,
		registry: reg,
	}
	err = ps.init(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to init ProviderStore, err: %v", err)
	}

	return ps, nil
}

func (ps *ProviderStore) init(ctx context.Context) error {
	if ps.ds == nil || ps.cs == nil {
		return fmt.Errorf("nil database")
	}
	root, err := ps.ds.Get(ctx, datastore.NewKey(rootKey))
	if err != nil {
		return err
	}

	// find root and load
	if err == nil && root != nil {
		_, rootcid, err := cid.CidFromBytes(root)
		if err != nil {
			return fmt.Errorf("failed to load ProviderStore root")
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

func (ps *ProviderStore) ProviderAddMeta(ctx context.Context, provider peer.ID, key string, metaContext []byte) error {
	c, err := cid.Decode(key)
	if err != nil {
		return fmt.Errorf("key must be valid cid, err :%v", err)
	}
	err = ps.registry.UpdateProviderInfo(ctx, provider, c, 0)
	if err != nil {
		return err
	}

	err = ps.root.Put(hamt.ProviderKey{
		ID:   provider,
		Meta: c,
	}, &types.MetaState{
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
