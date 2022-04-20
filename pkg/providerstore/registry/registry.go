package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"path"
	"sync"
)

const (
	// ProviderKeyPath is where provider info is stored in to indexer repo
	ProviderKeyPath = "/registry/ProviderInfo"
)

var log = logging.Logger("registry")

// ProviderInfo is an immutable data sturcture that holds information about a
// provider.  A ProviderInfo instance is never modified, but rather a new one
// is created to update its contents.  This means existing references remain
// valid.
type ProviderInfo struct {
	PeerID           peer.ID
	LastUpdateHeight uint64
	MetaList         []cid.Cid
}

func (p *ProviderInfo) DsKey() datastore.Key {
	return datastore.NewKey(path.Join(ProviderKeyPath, p.PeerID.String()))
}

type Registry struct {
	providers map[peer.ID]*ProviderInfo
	actions   chan func()
	closed    chan struct{}
	closeOnce sync.Once
	dstore    datastore.Batching
}

func New(ctx context.Context, ds datastore.Batching) (*Registry, error) {
	r := &Registry{
		providers: map[peer.ID]*ProviderInfo{},
		actions:   make(chan func()),
		closed:    make(chan struct{}),
		dstore:    ds,
	}

	count, err := r.loadPersistedProviders(ctx)
	if err != nil {
		return nil, err
	}

	log.Infow("loaded providers into registry", "count", count)

	go r.run()

	return r, nil
}

func (r *Registry) UpdateProviderInfo(ctx context.Context, provider peer.ID, addCid cid.Cid, lastUpdateHeight uint64) error {
	errCh := make(chan error, 1)
	r.actions <- func() {
		errCh <- r.syncUpdateProviderInfo(ctx, provider, addCid, lastUpdateHeight)
	}
	err := <-errCh
	if err != nil {
		return err
	}
	log.Infow("registered provider", "id", provider)
	return nil
}

func (r *Registry) syncUpdateProviderInfo(ctx context.Context, provider peer.ID, addCid cid.Cid, lastUpdateHeight uint64) error {
	info, exist := r.providers[provider]
	if !exist {
		info = new(ProviderInfo)
	}
	if addCid.Defined() {
		info.MetaList = append(info.MetaList, addCid)
	}
	if lastUpdateHeight != uint64(0) {
		info.LastUpdateHeight = lastUpdateHeight
	}
	err := r.syncPersistProvider(ctx, info)
	if err != nil {
		log.Errorf("could not persist provider info, err: %v", err)
		return fmt.Errorf("could not persist provider info, err: %v", err)
	}
	return nil
}

func (r *Registry) syncPersistProvider(ctx context.Context, info *ProviderInfo) error {
	value, err := json.Marshal(info)
	if err != nil {
		return err
	}

	dsKey := info.DsKey()
	if err = r.dstore.Put(ctx, dsKey, value); err != nil {
		return err
	}
	if err = r.dstore.Sync(ctx, dsKey); err != nil {
		return fmt.Errorf("cannot sync provider info: %s", err)
	}
	return nil
}

func (r *Registry) loadPersistedProviders(ctx context.Context) (int, error) {
	if r.dstore == nil {
		return 0, fmt.Errorf("nil datastore")
	}
	// Load all providers from the datastore.
	q := query.Query{
		Prefix: ProviderKeyPath,
	}
	results, err := r.dstore.Query(ctx, q)
	if err != nil {
		return 0, err
	}
	defer results.Close()
	var count int
	for result := range results.Next() {
		if result.Error != nil {
			return 0, fmt.Errorf("cannot read provider data: %v", result.Error)
		}
		ent := result.Entry

		peerID, err := peer.Decode(path.Base(ent.Key))
		if err != nil {
			return 0, fmt.Errorf("cannot decode provider ID: %s", err)
		}

		pinfo := new(ProviderInfo)
		err = json.Unmarshal(ent.Value, pinfo)
		if err != nil {
			return 0, err
		}

		r.providers[peerID] = pinfo
		count++
	}
	return count, nil
}

func (r *Registry) Close() error {
	var err error
	r.closeOnce.Do(func() {
		// wait for unfinished jobs
		done := make(chan struct{})
		r.actions <- func() {
			close(done)
		}
		<-done
		// close running goroutine
		close(r.actions)
		<-r.closed
		err = r.dstore.Close()
	})
	return err
}

func (r *Registry) run() {
	defer close(r.closed)

	for act := range r.actions {
		act()
	}
}
