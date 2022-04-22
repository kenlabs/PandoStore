package types

type StoreState int

const (
	Working StoreState = iota
	SnapShoting
)
