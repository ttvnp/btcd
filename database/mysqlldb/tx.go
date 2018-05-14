package myqlldb

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/internal/treap"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

type tx struct {
	managed        bool
	closed         bool
	writable       bool
	db             *db
	snapshot       *dbCacheSnapshot
	metaBucket     *bucket
	blockIdxBucket *bucket

	pendingBlocks    map[chainhash.Hash]int // key: block hash, value: pendingBlockData index.
	pendingBlockData []*btcutil.Block

	pendingKeys   *treap.Mutable
	pendingRemove *treap.Mutable

	activeIterLock sync.RWMutex
	activeIters    []*treap.Iterator
}

func (tx *tx) Metadata() database.Bucket {
	return tx.metaBucket
}

func (tx *tx) StoreBlock(block *btcutil.Block) error {
	if err := tx.checkClosed(); err != nil {
		return err
	}

	if !tx.writable {
		str := "store block requires a writable database transaction"
		return makeError(database.ErrTxNotWritable, str, nil)
	}

	blockHash := block.Hash()
	if tx.hasBlock(blockHash) {
		str := fmt.Sprintf("block %s already exists", blockHash)
		return makeError(database.ErrBlockExists, str, nil)
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		str := fmt.Sprintf("failed to get serialized bytes for block %s", blockHash)
		return makeError(database.ErrDriverSpecific, str, err)
	}
	copyBlock, _ := btcutil.NewBlockFromBytes(blockBytes) // make new block pointer
	copyBlock.SetHeight(block.Height())

	if tx.pendingBlocks == nil {
		tx.pendingBlocks = make(map[chainhash.Hash]int)
	}
	tx.pendingBlocks[*blockHash] = len(tx.pendingBlockData)
	tx.pendingBlockData = append(tx.pendingBlockData, copyBlock)
	log.Tracef("Added block %s to pending blocks", blockHash)

	return nil
}

func (tx *tx) HasBlock(hash *chainhash.Hash) (bool, error) {
	if err := tx.checkClosed(); err != nil {
		return false, err
	}

	return tx.hasBlock(hash), nil
}

func (tx *tx) HasBlocks(hashes []chainhash.Hash) ([]bool, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}
	results := make([]bool, len(hashes))
	for i := range hashes {
		results[i] = tx.hasBlock(&hashes[i])
	}
	return results, nil
}

func (tx *tx) FetchBlockHeader(hash *chainhash.Hash) ([]byte, error) {
	return tx.FetchBlockRegion(&database.BlockRegion{
		Hash:   hash,
		Offset: 0,
		Len:    wire.MaxBlockHeaderPayload,
	})
}

func (tx *tx) FetchBlockHeaders(hashes []chainhash.Hash) ([][]byte, error) {
	regions := make([]database.BlockRegion, len(hashes))
	for i := range hashes {
		regions[i].Hash = &hashes[i]
		regions[i].Offset = 0
		regions[i].Len = wire.MaxBlockHeaderPayload
	}
	return tx.FetchBlockRegions(regions)
}

func (tx *tx) FetchBlock(hash *chainhash.Hash) ([]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// read from pending blocks.
	if idx, exists := tx.pendingBlocks[*hash]; exists {
		return tx.pendingBlockData[idx].Bytes()
	}

	// read from storage.
	blockRow, err := tx.fetchBlockRow(hash)
	if err != nil {
		return nil, err
	}
	location := deserializeBlockLoc(blockRow)

	blockBytes, err := tx.db.store.readBlockByte(hash, location)
	if err != nil {
		return nil, err
	}

	return blockBytes, nil
}

func (tx *tx) FetchBlocks(hashes []chainhash.Hash) ([][]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	blocks := make([][]byte, len(hashes))
	for i := range hashes {
		var err error
		blocks[i], err = tx.FetchBlock(&hashes[i])
		if err != nil {
			return nil, err
		}
	}

	return blocks, nil
}

func (tx *tx) FetchBlockRegion(region *database.BlockRegion) ([]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// read from pending blocks.
	if tx.pendingBlocks != nil {
		regionBytes, err := tx.fetchPendingRegion(region)
		if err != nil {
			return nil, err
		}
		if regionBytes != nil {
			return regionBytes, nil
		}
	}

	// read from storage.
	blockRow, err := tx.fetchBlockRow(region.Hash)
	if err != nil {
		return nil, err
	}
	location := deserializeBlockLoc(blockRow)

	regionBytes, err := tx.db.store.readBlockRegion(location, region.Offset, region.Len)
	if err != nil {
		return nil, err
	}

	return regionBytes, nil
}

func (tx *tx) FetchBlockRegions(regions []database.BlockRegion) ([][]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	blockRegions := make([][]byte, len(regions))
	fetchList := make([]bulkFetchData, 0, len(regions))
	for i := range regions {
		region := &regions[i]

		// read from pending blocks.
		if tx.pendingBlocks != nil {
			regionBytes, err := tx.fetchPendingRegion(region)
			if err != nil {
				return nil, err
			}
			if regionBytes != nil {
				blockRegions[i] = regionBytes
				continue
			}
		}

		// read from storage.
		blockRow, err := tx.fetchBlockRow(region.Hash)
		if err != nil {
			return nil, err
		}
		location := deserializeBlockLoc(blockRow)
		fetchList = append(fetchList, bulkFetchData{&location, i})
	}
	sort.Sort(bulkFetchDataSorter(fetchList))

	// Read all of the regions in the fetch list and set the results.
	for i := range fetchList {
		fetchData := &fetchList[i]
		ri := fetchData.replyIndex
		region := &regions[ri]
		location := fetchData.blockLocation
		regionBytes, err := tx.db.store.readBlockRegion(*location, region.Offset, region.Len)
		if err != nil {
			return nil, err
		}
		blockRegions[ri] = regionBytes
	}

	return blockRegions, nil
}

func (tx *tx) Commit() error {
	if tx.managed {
		tx.close()
		panic("managed transaction commit not allowed")
	}

	if err := tx.checkClosed(); err != nil {
		return err
	}

	defer tx.close()

	if !tx.writable {
		str := "Commit requires a writable database transaction"
		return makeError(database.ErrTxNotWritable, str, nil)
	}

	return tx.writePendingAndCommit()
}

func (tx *tx) Rollback() error {
	if tx.managed {
		tx.close()
		panic("managed transaction rollback not allowed")
	}

	if err := tx.checkClosed(); err != nil {
		return err
	}

	tx.close()
	return nil
}

func (tx *tx) writePendingAndCommit() error {

	rollback := func() {
		tx.db.store.handleRollback()
	}

	// Loop through all of the pending blocks to store and write them.
	for _, block := range tx.pendingBlockData {
		blockhash := block.Hash()
		log.Tracef("Storing block %s", blockhash)
		location, err := tx.db.store.writeBlock(block)
		if err != nil {
			rollback()
			return err
		}

		// Add a record in the block index for the block.
		blockRow := serializeBlockLoc(location)
		err = tx.blockIdxBucket.Put(blockhash[:], blockRow)
		if err != nil {
			rollback()
			return err
		}
	}
	// commit block store
	if err := tx.db.store.commit(); err != nil {
		return err
	}

	// commit metadata.
	return tx.db.cache.commitTx(tx)
}

func (tx *tx) checkClosed() error {
	if tx.closed {
		return makeError(database.ErrTxClosed, "database transaction is closed", nil)
	}
	return nil
}

func (tx *tx) close() {
	tx.closed = true

	tx.pendingBlocks = nil
	tx.pendingBlockData = nil

	tx.pendingKeys = nil
	tx.pendingRemove = nil

	// close cache db
	if tx.snapshot != nil {
		tx.snapshot.Release()
		tx.snapshot = nil
	}

	// close block store db
	tx.db.store.close()

	// unlock mutex
	tx.db.closeLock.RUnlock()
	if tx.writable {
		tx.db.writeLock.Unlock()
	}
}

func (tx *tx) nextBucketID() ([4]byte, error) {
	// load highest
	curIDBytes := tx.fetchKey(curBucketIDKeyName)
	curBucketNum := binary.BigEndian.Uint32(curIDBytes)

	//
	var nextBucketID [4]byte
	binary.BigEndian.PutUint32(nextBucketID[:], curBucketNum+1)
	if err := tx.putKey(curBucketIDKeyName, nextBucketID[:]); err != nil {
		return [4]byte{}, err
	}
	return nextBucketID, nil
}

func (tx *tx) hasBlock(hash *chainhash.Hash) bool {
	if _, exists := tx.pendingBlocks[*hash]; exists {
		return true
	}
	return tx.hasKey(bucketizedKey(blockIdxBucketID, hash[:]))
}

func (tx *tx) fetchBlockRow(hash *chainhash.Hash) ([]byte, error) {
	blockRow := tx.blockIdxBucket.Get(hash[:])
	if blockRow == nil {
		str := fmt.Sprintf("block %s does not exist", hash)
		return nil, makeError(database.ErrBlockNotFound, str, nil)
	}

	return blockRow, nil
}

func (tx *tx) fetchPendingRegion(region *database.BlockRegion) ([]byte, error) {
	idx, exists := tx.pendingBlocks[*region.Hash]
	if !exists {
		return nil, nil
	}

	blockBytes, _ := tx.pendingBlockData[idx].Bytes()
	blockLen := uint32(len(blockBytes))
	endOffset := region.Offset + region.Len
	if endOffset < region.Offset || endOffset > blockLen {
		str := fmt.Sprintf("block %s region offset %d, length %d "+
			"exceeds block length of %d", region.Hash,
			region.Offset, region.Len, blockLen)
		return nil, makeError(database.ErrBlockRegionInvalid, str, nil)
	}

	return blockBytes[region.Offset:endOffset:endOffset], nil
}

func (tx *tx) hasKey(key []byte) bool {
	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return false
		}
		if tx.pendingKeys.Has(key) {
			return true
		}
	}

	return tx.snapshot.Has(key)
}

func (tx *tx) fetchKey(key []byte) []byte {
	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return nil
		}
		if value := tx.pendingKeys.Get(key); value != nil {
			return value
		}
	}
	return tx.snapshot.Get(key)
}

func (tx *tx) putKey(key, value []byte) error {
	tx.pendingRemove.Delete(key)
	tx.pendingKeys.Put(key, value)
	tx.notifyActiveIters()
	return nil
}

func (tx *tx) deleteKey(key []byte, notifyIterators bool) {
	tx.pendingKeys.Delete(key)
	tx.pendingRemove.Put(key, nil)
	if notifyIterators {
		tx.notifyActiveIters()
	}
}

func (tx *tx) notifyActiveIters() {
	tx.activeIterLock.RLock()
	for _, iter := range tx.activeIters {
		iter.ForceReseek()
	}
	tx.activeIterLock.RUnlock()
}

func (tx *tx) removeActiveIter(iter *treap.Iterator) {
	tx.activeIterLock.Lock()
	for i := 0; i < len(tx.activeIters); i++ {
		if tx.activeIters[i] == iter {
			copy(tx.activeIters[i:], tx.activeIters[i+1:])
			tx.activeIters[len(tx.activeIters)-1] = nil
			tx.activeIters = tx.activeIters[:len(tx.activeIters)-1]
		}
	}
	tx.activeIterLock.Unlock()
}

func (tx *tx) addActiveIter(iter *treap.Iterator) {
	tx.activeIterLock.Lock()
	tx.activeIters = append(tx.activeIters, iter)
	tx.activeIterLock.Unlock()
}
