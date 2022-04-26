package config

import "github.com/mitchellh/go-homedir"

const DefaultStoreDir = "PandoStore"

type StoreConfig struct {
	Type             string
	StoreRoot        string
	Dir              string
	SnapShotInterval string
}

func DefaultConfig() *StoreConfig {
	homeRoot, _ := homedir.Expand("~/.pando")

	return &StoreConfig{
		Type:             "levelds",
		StoreRoot:        homeRoot,
		Dir:              DefaultStoreDir,
		SnapShotInterval: "60m",
	}
}
