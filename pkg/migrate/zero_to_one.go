package migrate

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsns "github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	dtsync "github.com/ipfs/go-datastore/sync"
	leveldb "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/kenlabs/PandoStore/pkg"
	"github.com/kenlabs/PandoStore/pkg/metastore"
	"os"
	"path"
)

func migrateDBFromZeroToOne(oldDsDir string, deleteOldStore bool) error {
	onlyReadOldDB, err := leveldb.NewDatastore(oldDsDir, nil)
	if err != nil {
		return err
	}
	mds := dtsync.MutexWrap(onlyReadOldDB)
	bs := blockstore.NewBlockstore(mds)

	tmpNewDBPath := path.Join(path.Dir(oldDsDir), "."+path.Base(oldDsDir))
	newBaseDb, err := leveldb.NewDatastore(tmpNewDBPath, nil)
	if err != nil {
		return err
	}
	newMds := dtsync.MutexWrap(newBaseDb)
	newMetaDs := dsns.Wrap(newMds, metastore.MetaPrefix)

	ch, err := bs.AllKeysChan(context.Background())
	if err != nil {
		return err
	}

	// migrate ipld nodes from block store
Flag:
	for {
		select {
		case c, exist := <-ch:
			fmt.Printf("%s", c.String())
			if !exist {
				break Flag
			}
			block, err := bs.Get(context.Background(), c)
			if err != nil {
				return err
			}
			err = newMetaDs.Put(context.Background(), dshelp.MultihashToDsKey(c.Hash()), block.RawData())
			if err != nil {
				return err
			}
		}
	}

	q := query.Query{
		Prefix: "/sync/",
	}
	qres, err := mds.Query(context.Background(), q)
	if err != nil {
		return err
	}

	for result := range qres.Next() {
		if result.Error != nil {
			return result.Error
		}
		ent := result.Entry
		_, _, err = cid.CidFromBytes(ent.Value)
		if err != nil {
			log.Debugf("invalid latest sync cid for provider: %s, skip\n", path.Base(result.Entry.Key))
			continue
		}
		err = newMds.Put(context.Background(), datastore.NewKey(ent.Key), ent.Value)
		if err != nil {
			return err
		}
	}

	err = mds.Close()
	if err != nil {
		log.Errorf("failed to close the old db after migrating, err :%v", err)
		return err
	}
	err = newMds.Close()
	if err != nil {
		log.Errorf("failed to close the new db after migrating, err :%v", err)
		return err
	}

	if !deleteOldStore {
		err = os.Rename(oldDsDir, path.Join(path.Dir(oldDsDir), "_"+path.Base(oldDsDir)))
		if err != nil {
			log.Errorf("failed to back up the old store dir, err: %v", err)
			return err
		}
	} else {
		err = os.RemoveAll(oldDsDir)
		if err != nil {
			log.Errorf("failed to delete old store dir, err: %v", err)
			return err
		}
	}

	err = os.Rename(tmpNewDBPath, oldDsDir)
	if err != nil {
		log.Errorf("failed to replace old store dir with new dir, err: %v", err)
		return err
	}

	file, err := os.OpenFile(path.Join(oldDsDir, "version"), os.O_RDWR|os.O_CREATE, 0644)
	defer file.Close()
	if err != nil {
		log.Errorf("failed to create version file, err: %v", err)
		return err
	}

	_, err = file.WriteString(pkg.Version1)
	if err != nil {
		log.Errorf("failed to write version info: version: %s, err: %v", pkg.Version1, err)
		return err
	}

	return nil
}
