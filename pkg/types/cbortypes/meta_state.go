package cbortypes

//go:generate cbor-gen-for MetaState
type MetaState struct {
	ProviderID     string
	SnapShotCid    string
	SnapShotHeight uint64
	Context        []byte
}
