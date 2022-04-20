package pkg

import "context"

type PandoStore interface {
	Store(ctx context.Context, key string, val []byte, provider string, metaContext []byte, prev []byte) error
}
