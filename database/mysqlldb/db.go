package myqlldb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/internal/treap"
	"github.com/btcsuite/btcd/database/mysqlldb/mysql"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/filter"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

const (
	// metadataDbName is the name used for the metadata database.
	metadataDbName = "metadata"
)

var (
	// byteOrder is the preferred byte order used through the database and block files.
	byteOrder = binary.LittleEndian
)

type db struct {
	writeLock sync.Mutex
	closeLock sync.RWMutex
	closed    bool
	store     *blockStore // Data layer which wraps underlying mysql DB.
	cache     *dbCache    // Cache layer which wraps underlying leveldb DB.
}

func (db *db) Type() string {
	return dbType
}

func (db *db) Begin(writable bool) (database.Tx, error) {
	return db.begin(writable)
}

func (db *db) View(fn func(database.Tx) error) error {
	tx, err := db.begin(false)
	if err != nil {
		return err
	}

	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Rollback()
}

func (db *db) Update(fn func(database.Tx) error) error {
	tx, err := db.begin(true)
	if err != nil {
		return err
	}

	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (db *db) Close() error {
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return makeError(database.ErrDbNotOpen, errDbNotOpenStr, nil)
	}
	db.closed = true

	cacheCloseErr := db.cache.Close()
	db.store.close()

	return cacheCloseErr
}

func (db *db) begin(writable bool) (*tx, error) {
	if writable {
		db.writeLock.Lock()
	}

	db.closeLock.RLock()
	if db.closed {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, makeError(database.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	snapshot, err := db.cache.Snapshot()
	if err != nil {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, err
	}

	err = db.store.begin(writable)
	if err != nil {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, err
	}

	tx := &tx{
		writable:      writable,
		db:            db,
		snapshot:      snapshot,
		pendingKeys:   treap.NewMutable(),
		pendingRemove: treap.NewMutable(),
	}
	tx.metaBucket = &bucket{tx: tx, id: metadataBucketID}
	tx.blockIdxBucket = &bucket{tx: tx, id: blockIdxBucketID}
	return tx, nil
}

func rollbackOnPanic(tx *tx) {
	if err := recover(); err != nil {
		tx.managed = false
		_ = tx.Rollback()
		panic(err)
	}
}

func initDB(ldb *leveldb.DB) error {

	batch := new(leveldb.Batch)
	batch.Put(bucketIndexKey(metadataBucketID, blockIdxBucketName), blockIdxBucketID[:])
	batch.Put(curBucketIDKeyName, blockIdxBucketID[:])

	if err := ldb.Write(batch, nil); err != nil {
		str := fmt.Sprintf("failed to initialize metadata database: %v", err)
		return convertErr(str, err)
	}

	// TODO mysql scheme version control here.

	return nil
}

func openDB(
	dbPath string,
	readWriteConfig *mysql.Config,
	readOnlyConfig *mysql.Config,
	network wire.BitcoinNet,
	create bool,
) (database.DB, error) {
	metadataDbPath := filepath.Join(dbPath, metadataDbName)
	dbExists := fileExists(metadataDbPath)
	if !create && !dbExists {
		str := fmt.Sprintf("database %q does not exist", metadataDbPath)
		return nil, makeError(database.ErrDbDoesNotExist, str, nil)
	}

	if !dbExists {
		_ = os.MkdirAll(dbPath, 0700)
	}

	// Open the metadata database (will create it if needed).
	opts := opt.Options{
		ErrorIfExist: create,
		Strict:       opt.DefaultStrict,
		Compression:  opt.NoCompression,
		Filter:       filter.NewBloomFilter(10),
	}
	ldb, err := leveldb.OpenFile(metadataDbPath, &opts)
	if err != nil {
		return nil, convertErr(err.Error(), err)
	}

	// Create the block store.
	store := newBlockStore(network, readWriteConfig, readOnlyConfig)
	if err := store.tryConnect(); err != nil {
		return nil, err
	}
	cache := newDbCache(ldb, store, defaultCacheSize, defaultFlushSecs)
	pdb := &db{store: store, cache: cache}

	if create {
		if err := initDB(pdb.cache.ldb); err != nil {
			return nil, err
		}
	}

	return pdb, nil
}
