package myqlldb

import (
	"github.com/btcsuite/btcd/database"
	"bytes"
	"fmt"
	"runtime"
)

var (
	// bucketIndexPrefix is the prefix used for all entries in the bucket index.
	bucketIndexPrefix = []byte("bidx")

	// curBucketIDKeyName is the name of the key used to keep track of the
	// current bucket ID counter.
	curBucketIDKeyName = []byte("bidx-cbid")

	// metadataBucketID is the ID of the top-level metadata bucket.
	// It is the value 0 encoded as an unsigned big-endian uint32.
	metadataBucketID = [4]byte{}

	// blockIdxBucketID is the ID of the internal block metadata bucket.
	// It is the value 1 encoded as an unsigned big-endian uint32.
	blockIdxBucketID = [4]byte{0x00, 0x00, 0x00, 0x01}

	// blockIdxBucketName is the bucket used internally to track block metadata.
	blockIdxBucketName = []byte("mysqlldb-blockidx")
)

type bucket struct {
	tx *tx
	id [4]byte
}

func (b *bucket) Bucket(key []byte) database.Bucket {
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	childID := b.tx.fetchKey(bucketIndexKey(b.id, key))
	if childID == nil {
		return nil
	}

	childBucket := &bucket{tx: b.tx}
	copy(childBucket.id[:], childID)
	return childBucket
}

func (b *bucket) CreateBucket(key []byte) (database.Bucket, error)  {
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeError(database.ErrTxNotWritable, str, nil)
	}

	if len(key) == 0 {
		str := "create bucket requires a key"
		return nil, makeError(database.ErrBucketNameRequired, str, nil)
	}

	bidxKey := bucketIndexKey(b.id, key)
	if b.tx.hasKey(bidxKey) {
		str := "bucket already exists"
		return nil, makeError(database.ErrBucketExists, str, nil)
	}

	var childID [4]byte
	if b.id == metadataBucketID && bytes.Equal(key, blockIdxBucketName) {
		// In the case of the special internal block index, keep the fixed ID.
		childID = blockIdxBucketID
	} else {
		// Find the appropriate next bucket ID to use for the new bucket.
		var err error
		childID, err = b.tx.nextBucketID()
		if err != nil {
			return nil, err
		}
	}

	// Add the new bucket to the bucket index.
	if err := b.tx.putKey(bidxKey, childID[:]); err != nil {
		str := fmt.Sprintf("failed to create bucket with key %q", key)
		return nil, convertErr(str, err)
	}
	return &bucket{tx: b.tx, id: childID}, nil
}

func (b *bucket) CreateBucketIfNotExists(key []byte) (database.Bucket, error) {
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeError(database.ErrTxNotWritable, str, nil)
	}

	if bucket := b.Bucket(key); bucket != nil {
		return bucket, nil
	}
	return b.CreateBucket(key)
}

func (b *bucket) DeleteBucket(key []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	if !b.tx.writable {
		str := "delete bucket requires a writable database transaction"
		return makeError(database.ErrTxNotWritable, str, nil)
	}

	bidxKey := bucketIndexKey(b.id, key)
	childID := b.tx.fetchKey(bidxKey)
	if childID == nil {
		str := fmt.Sprintf("bucket %q does not exist", key)
		return makeError(database.ErrBucketNotFound, str, nil)
	}

	// Remove all nested buckets and their keys.
	childIDs := [][]byte{childID}
	for len(childIDs) > 0 {
		childID = childIDs[len(childIDs)-1]
		childIDs = childIDs[:len(childIDs)-1]

		keyCursor := newCursor(b, childID, ctKeys)
		for ok := keyCursor.First(); ok; ok = keyCursor.Next() {
			b.tx.deleteKey(keyCursor.rawKey(), false)
		}
		cursorFinalizer(keyCursor)

		// Iterate through all nested buckets.
		bucketCursor := newCursor(b, childID, ctBuckets)
		for ok := bucketCursor.First(); ok; ok = bucketCursor.Next() {
			childID := bucketCursor.rawValue()
			childIDs = append(childIDs, childID)
			b.tx.deleteKey(bucketCursor.rawKey(), false)
		}
		cursorFinalizer(bucketCursor)
	}

	// Remove the nested bucket from the bucket index.  Any buckets nested
	// under it were already removed above.
	b.tx.deleteKey(bidxKey, true)
	return nil
}

func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	c := newCursor(b, b.id[:], ctKeys)
	defer cursorFinalizer(c)
	for ok := c.First(); ok; ok = c.Next() {
		err := fn(c.Key(), c.Value())
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *bucket) ForEachBucket(fn func(k []byte) error) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	c := newCursor(b, b.id[:], ctBuckets)
	defer cursorFinalizer(c)
	for ok := c.First(); ok; ok = c.Next() {
		err := fn(c.Key())
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *bucket) Cursor() database.Cursor {
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	c := newCursor(b, b.id[:], ctFull)
	runtime.SetFinalizer(c, cursorFinalizer) // released on garbage collection.
	return c
}

func (b *bucket) Writable() bool {

	return b.tx.writable
}

func (b *bucket) Put(key, value []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	if !b.tx.writable {
		str := "setting a key requires a writable database transaction"
		return makeError(database.ErrTxNotWritable, str, nil)
	}

	if len(key) == 0 {
		str := "put requires a key"
		return makeError(database.ErrKeyRequired, str, nil)
	}

	return b.tx.putKey(bucketizedKey(b.id, key), value)
}

func (b *bucket) Get(key []byte) []byte {
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	if len(key) == 0 {
		return nil
	}

	return b.tx.fetchKey(bucketizedKey(b.id, key))
}

func (b *bucket) Delete(key []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	if !b.tx.writable {
		str := "deleting a value requires a writable database transaction"
		return makeError(database.ErrTxNotWritable, str, nil)
	}

	if len(key) == 0 {
		return nil
	}

	b.tx.deleteKey(bucketizedKey(b.id, key), true)
	return nil
}
