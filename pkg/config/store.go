package config

const DefaultStoreDir = "PandoStore"

type StoreConfig struct {
	Type             string
	StoreRoot        string
	Dir              string
	SnapShotInterval string
}
