package myqlldb

import (
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/btcd/database/internal/treap"
	"github.com/btcsuite/goleveldb/leveldb/util"
	"github.com/btcsuite/goleveldb/leveldb/iterator"
	"bytes"
	"time"
	"sync"
	"fmt"
)

const (
	// defaultCacheSize is the default size for the database cache.
	defaultCacheSize = 100 * 1024 * 1024 // 100 MB

	// defaultFlushSecs is the default number of seconds to use as a
	// threshold in between database cache flushes when the cache size has
	// not been exceeded.
	defaultFlushSecs = 300 // 5 minutes
)

type dbCache struct {

	ldb *leveldb.DB // level db for metadata.

	store *blockStore // db for store block data.

	maxSize       uint64
	flushInterval time.Duration
	lastFlush     time.Time

	cacheLock    sync.RWMutex
	cachedKeys   *treap.Immutable
	cachedRemove *treap.Immutable
}

func newDbCache(ldb *leveldb.DB, store *blockStore, maxSize uint64, flushIntervalSecs uint32) *dbCache {
	return &dbCache{
		ldb:           ldb,
		store:         store,
		maxSize:       maxSize,
		flushInterval: time.Second * time.Duration(flushIntervalSecs),
		lastFlush:     time.Now(),
		cachedKeys:    treap.NewImmutable(),
		cachedRemove:  treap.NewImmutable(),
	}
}

func (c *dbCache) Snapshot() (*dbCacheSnapshot, error) {
	dbSnapshot, err := c.ldb.GetSnapshot()
	if err != nil {
		str := "failed to open transaction"
		return nil, convertErr(str, err)
	}

	c.cacheLock.RLock()
	cacheSnapshot := &dbCacheSnapshot{
		dbSnapshot:    dbSnapshot,
		pendingKeys:   c.cachedKeys,
		pendingRemove: c.cachedRemove,
	}
	c.cacheLock.RUnlock()
	return cacheSnapshot, nil
}

func (c *dbCache) Close() error {

	if err := c.flush(); err != nil {
		_ = c.ldb.Close()
		return err
	}

	if err := c.ldb.Close(); err != nil {
		str := "failed to close underlying leveldb database"
		return convertErr(str, err)
	}

	return nil
}

func (c *dbCache) commitTx(tx *tx) error {
	if c.needsFlush(tx) {
		// case needs flush, flush to level db
		if err := c.flush(); err != nil {
			return err
		}

		err := c.commitTreaps(tx.pendingKeys, tx.pendingRemove)
		if err != nil {
			return err
		}

		tx.pendingKeys = nil
		tx.pendingRemove = nil
		return nil
	}

	// pending -> in-memory cache.
	c.cacheLock.RLock()
	newCachedKeys := c.cachedKeys
	newCachedRemove := c.cachedRemove
	c.cacheLock.RUnlock()

	tx.pendingKeys.ForEach(func(k, v []byte) bool {
		newCachedRemove = newCachedRemove.Delete(k)
		newCachedKeys = newCachedKeys.Put(k, v)
		return true
	})
	tx.pendingKeys = nil

	tx.pendingRemove.ForEach(func(k, v []byte) bool {
		newCachedKeys = newCachedKeys.Delete(k)
		newCachedRemove = newCachedRemove.Put(k, nil)
		return true
	})
	tx.pendingRemove = nil

	c.cacheLock.Lock()
	c.cachedKeys = newCachedKeys
	c.cachedRemove = newCachedRemove
	c.cacheLock.Unlock()
	return nil
}

func (c *dbCache) flush() error {
	c.lastFlush = time.Now()

	// flush block data to storage.
	if err := c.store.syncBlocks(); err != nil {
		return err
	}

	// flush metadata to ldb.
	c.cacheLock.RLock()
	cachedKeys := c.cachedKeys
	cachedRemove := c.cachedRemove
	c.cacheLock.RUnlock()

	if cachedKeys.Len() == 0 && cachedRemove.Len() == 0 {
		return nil
	}

	if err := c.commitTreaps(cachedKeys, cachedRemove); err != nil {
		return err
	}

	c.cacheLock.Lock()
	c.cachedKeys = treap.NewImmutable()
	c.cachedRemove = treap.NewImmutable()
	c.cacheLock.Unlock()

	return nil
}

func (c *dbCache) needsFlush(tx *tx) bool {
	if time.Since(c.lastFlush) > c.flushInterval {
		return true
	}
	snap := tx.snapshot
	totalSize := snap.pendingKeys.Size() + snap.pendingRemove.Size()
	totalSize = uint64(float64(totalSize) * 1.5)
	return totalSize > c.maxSize
}


type TreapForEacher interface {
	ForEach(func(k, v []byte) bool)
}

func (c *dbCache) commitTreaps(pendingKeys, pendingRemove TreapForEacher) error {

	return c.updateDB(func(ldbTx *leveldb.Transaction) error {
		var innerErr error
		pendingKeys.ForEach(func(k, v []byte) bool {
			if dbErr := ldbTx.Put(k, v, nil); dbErr != nil {
				str := fmt.Sprintf("failed to put key %q to ldb transaction", k)
				innerErr = convertErr(str, dbErr)
				return false
			}
			return true
		})
		if innerErr != nil {
			return innerErr
		}

		pendingRemove.ForEach(func(k, v []byte) bool {
			if dbErr := ldbTx.Delete(k, nil); dbErr != nil {
				str := fmt.Sprintf("failed to delete key %q from ldb transaction", k)
				innerErr = convertErr(str, dbErr)
				return false
			}
			return true
		})
		return innerErr
	})
}

func (c *dbCache) updateDB(fn func(ldbTx *leveldb.Transaction) error) error {

	ldbTx, err := c.ldb.OpenTransaction()
	if err != nil {
		return convertErr("failed to open ldb transaction", err)
	}

	if err := fn(ldbTx); err != nil {
		ldbTx.Discard()
		return err
	}

	if err := ldbTx.Commit(); err != nil {
		return convertErr("failed to commit leveldb transaction", err)
	}
	return nil
}


type dbCacheSnapshot struct {
	dbSnapshot    *leveldb.Snapshot
	pendingKeys   *treap.Immutable
	pendingRemove *treap.Immutable
}

func (snap *dbCacheSnapshot) NewIterator(slice *util.Range) *dbCacheIterator {
	return &dbCacheIterator{
		dbIter:        snap.dbSnapshot.NewIterator(slice, nil),
		cacheIter:     newLdbCacheIter(snap, slice),
		cacheSnapshot: snap,
	}
}

func (snap *dbCacheSnapshot) Release() {
	snap.dbSnapshot.Release()
	snap.pendingKeys = nil
	snap.pendingRemove = nil
}

func (snap *dbCacheSnapshot) Has(key []byte) bool {
	if snap.pendingRemove.Has(key) {
		return false
	}
	if snap.pendingKeys.Has(key) {
		return true
	}
	hasKey, _ := snap.dbSnapshot.Has(key, nil)
	return hasKey
}

func (snap *dbCacheSnapshot) Get(key []byte) []byte {
	if snap.pendingRemove.Has(key) {
		return nil
	}
	if value := snap.pendingKeys.Get(key); value != nil {
		return value
	}
	value, err := snap.dbSnapshot.Get(key, nil)
	if err != nil {
		return nil
	}
	return value
}


/** iterator for cache store. **/
type dbCacheIterator struct {
	cacheSnapshot *dbCacheSnapshot
	dbIter        iterator.Iterator // ldb iterator
	cacheIter     iterator.Iterator // treap iterator for in-memory cache.
	currentIter   iterator.Iterator
	released      bool
}

func (i *dbCacheIterator) First() bool {
	i.dbIter.First()
	i.cacheIter.First()
	return i.chooseIterator(true)
}

func (i *dbCacheIterator) Last() bool {
	i.dbIter.Last()
	i.cacheIter.Last()
	return i.chooseIterator(false)
}

func (i *dbCacheIterator) Next() bool {
	if i.currentIter == nil {
		return false
	}
	i.currentIter.Next()
	return i.chooseIterator(true)
}

func (i *dbCacheIterator) Prev() bool {
	if i.currentIter == nil {
		return false
	}
	i.currentIter.Prev()
	return i.chooseIterator(false)
}

func (i *dbCacheIterator) Seek(key []byte) bool {
	i.dbIter.Seek(key)
	i.cacheIter.Seek(key)
	return i.chooseIterator(true)
}

func (i *dbCacheIterator) Valid() bool {
	return i.currentIter != nil
}

func (i *dbCacheIterator) Key() []byte {
	if i.currentIter == nil {
		return nil
	}
	return i.currentIter.Key()
}

func (i *dbCacheIterator) Value() []byte {
	if i.currentIter == nil {
		return nil
	}
	return i.currentIter.Value()
}

func (i *dbCacheIterator) SetReleaser(releaser util.Releaser) {
}

func (i *dbCacheIterator) Release() {
	if !i.released {
		i.dbIter.Release()
		i.cacheIter.Release()
		i.currentIter = nil
		i.released = true
	}
}

func (i *dbCacheIterator) Error() error {
	return nil
}

func (i *dbCacheIterator) skipPendingUpdates(forwards bool) {
	for i.dbIter.Valid() {
		var skip bool
		key := i.dbIter.Key()
		if i.cacheSnapshot.pendingRemove.Has(key) {
			skip = true
		} else if i.cacheSnapshot.pendingKeys.Has(key) {
			skip = true
		}
		if !skip {
			break
		}

		if forwards {
			i.dbIter.Next()
		} else {
			i.dbIter.Prev()
		}
	}
}

func (i *dbCacheIterator) chooseIterator(forwards bool) bool {
	i.skipPendingUpdates(forwards)

	if !i.dbIter.Valid() && !i.cacheIter.Valid() {
		i.currentIter = nil
		return false
	}

	if !i.cacheIter.Valid() {
		i.currentIter = i.dbIter
		return true
	}

	if !i.dbIter.Valid() {
		i.currentIter = i.cacheIter
		return true
	}

	compare := bytes.Compare(i.dbIter.Key(), i.cacheIter.Key())
	if (forwards && compare > 0) || (!forwards && compare < 0) {
		i.currentIter = i.cacheIter
	} else {
		i.currentIter = i.dbIter
	}
	return true
}


/**** iterator for level db cache ****/
type ldbCacheIter struct {
	*treap.Iterator
}

func (i *ldbCacheIter) Error() error {
	return nil
}

func (i *ldbCacheIter) SetReleaser(releaser util.Releaser) {
}

func (i *ldbCacheIter) Release() {
}

func newLdbCacheIter(snap *dbCacheSnapshot, slice *util.Range) *ldbCacheIter {
	iter := snap.pendingKeys.Iterator(slice.Start, slice.Limit)
	return &ldbCacheIter{Iterator: iter}
}
