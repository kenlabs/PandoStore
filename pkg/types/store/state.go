package store

type StoreState int

const (
	Working StoreState = iota
	SnapShoting
)
