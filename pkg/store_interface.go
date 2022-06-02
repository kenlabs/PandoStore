package pkg

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/kenlabs/PandoStore/pkg/snapshotstore"
	"github.com/kenlabs/PandoStore/pkg/types/store"
	"github.com/libp2p/go-libp2p-core/peer"
)

type PandoStore interface {
	Store(ctx context.Context, key cid.Cid, val []byte, provider peer.ID, metaContext []byte) error
	Get(ctx context.Context, key cid.Cid) ([]byte, error)
	MetaInclusion(ctx context.Context, c cid.Cid) (*store.MetaInclusion, error)
	SnapShotStore() *snapshotstore.SnapShotStore
	Close() error
}
