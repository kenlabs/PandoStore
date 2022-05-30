package pkg

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

const (
	CurrentVersion  = Version1
	VersionFileName = "version"
	Version1        = "PandoStore1.0"
	Version0        = "PandoStore0.1"
)

var ErrUnknownVer = fmt.Errorf("Unknown version")

func CheckVersion(storePath string) (string, error) {
	versionFilePath := path.Join(storePath, VersionFileName)
	versionFile, err := os.OpenFile(versionFilePath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return Version0, nil
		}
		return "", err
	}
	verInfo, err := ioutil.ReadAll(versionFile)
	if err != nil {
		return "", err
	}

	verStr := string(verInfo)
	fmt.Printf("%s", verStr)
	switch verStr {
	case Version1:
		return Version1, nil
	case Version0:
		return Version0, nil
	default:
		return "", ErrUnknownVer
	}
}
