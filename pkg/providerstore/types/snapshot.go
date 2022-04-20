package types

import "github.com/ipfs/go-cid"

//go:generate cbor-gen-for Metalist SnapShot
type SnapShot struct {
	Update       map[string]*Metalist
	Height       uint64
	CreateTime   uint64
	PrevSnapShot string
}

type Metalist struct {
	MetaList []cid.Cid
}
