package migrate

import (
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/kenlabs/pando-store/pkg"
)

var (
	ErrInvalidVersionUpgrade = fmt.Errorf("invalid version to upgrade")
)

var log = logging.Logger("migrate")

func Migrate(oldVer string, newVer string, storePath string, delOldStore bool) error {
	var err error
	switch oldVer {
	case pkg.Version0:
		if newVer == pkg.Version1 {
			err = migrateDBFromZeroToOne(storePath, delOldStore)
			break
		} else {
			err = ErrInvalidVersionUpgrade
		}
	default:
		err = ErrInvalidVersionUpgrade
	}

	return err
}
