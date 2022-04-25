package pkg

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
)

type PandoStore interface {
	Store(ctx context.Context, key string, val []byte, provider peer.ID, metaContext []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}
