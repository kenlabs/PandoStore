package store

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type MetaInclusion struct {
	ID             cid.Cid `json:"ID"`
	Provider       peer.ID `json:"Provider"`
	InPando        bool    `json:"InPando"`
	InSnapShot     bool    `json:"InSnapShot"`
	SnapShotID     cid.Cid `json:"SnapShotID"`
	SnapShotHeight uint64  `json:"SnapShotHeight"`
	Context        []byte  `json:"Context"`
	TranscationID  int     `json:"TranscationID"`
}
