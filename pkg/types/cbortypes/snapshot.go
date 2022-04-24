package cbortypes

import (
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
)

var LinkProto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagJson),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: 16,
	},
}

//go:generate cbor-gen-for Metalist SnapShot
type SnapShot struct {
	Update       map[string]*Metalist
	StateRoot    cid.Cid
	Height       uint64
	CreateTime   uint64
	PrevSnapShot string
}

type Metalist struct {
	MetaList []cid.Cid
}
