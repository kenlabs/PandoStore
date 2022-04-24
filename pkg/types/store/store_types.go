package store

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type MetaInclusion struct {
	ID             cid.Cid
	Provider       peer.ID
	InPando        bool
	InSnapShot     bool
	SnapShotID     cid.Cid
	SnapShotHeight uint64
	Context        []byte
	TranscationID  int
}
