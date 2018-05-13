package myqlldb

import (
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/goleveldb/leveldb/iterator"
	"bytes"
	"github.com/btcsuite/goleveldb/leveldb/util"
	"github.com/btcsuite/goleveldb/leveldb/comparer"
)

type cursorType int

const (
	// ctKeys iterates through all of the keys in a given bucket.
	ctKeys cursorType = iota

	// ctBuckets iterates through all directly nested buckets in a given bucket.
	ctBuckets

	// ctFull iterates through both the keys and the directly nested buckets in a given bucket.
	ctFull
)

type cursor struct {
	bucket      *bucket
	dbIter      iterator.Iterator
	pendingIter iterator.Iterator
	currentIter iterator.Iterator
}

// newCursor returns a new cursor for the given bucket, bucket ID, and cursor type.
//
// NOTE: The caller is responsible for calling the cursorFinalizer function on
// the returned cursor.
func newCursor(b *bucket, bucketID []byte, cursorTyp cursorType) *cursor {
	var dbIter, pendingIter iterator.Iterator
	switch cursorTyp {
	case ctKeys:
		keyRange := util.BytesPrefix(bucketID)
		dbIter = b.tx.snapshot.NewIterator(keyRange)
		pendingKeyIter := newLdbTreapIter(b.tx, keyRange)
		pendingIter = pendingKeyIter

	case ctBuckets:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>

		// Create an iterator for the both the database and the pending
		// keys which are prefixed by the bucket index identifier and
		// the provided bucket ID.
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := util.BytesPrefix(prefix)

		dbIter = b.tx.snapshot.NewIterator(bucketRange)
		pendingBucketIter := newLdbTreapIter(b.tx, bucketRange)
		pendingIter = pendingBucketIter

	case ctFull:
		fallthrough
	default:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := util.BytesPrefix(prefix)
		keyRange := util.BytesPrefix(bucketID)

		// Since both keys and buckets are needed from the database,
		// create an individual iterator for each prefix and then create
		// a merged iterator from them.
		dbKeyIter := b.tx.snapshot.NewIterator(keyRange)
		dbBucketIter := b.tx.snapshot.NewIterator(bucketRange)
		iters := []iterator.Iterator{dbKeyIter, dbBucketIter}
		dbIter = iterator.NewMergedIterator(iters,
			comparer.DefaultComparer, true)

		// Since both keys and buckets are needed from the pending keys,
		// create an individual iterator for each prefix and then create
		// a merged iterator from them.
		pendingKeyIter := newLdbTreapIter(b.tx, keyRange)
		pendingBucketIter := newLdbTreapIter(b.tx, bucketRange)
		iters = []iterator.Iterator{pendingKeyIter, pendingBucketIter}
		pendingIter = iterator.NewMergedIterator(iters,
			comparer.DefaultComparer, true)
	}

	return &cursor{bucket: b, dbIter: dbIter, pendingIter: pendingIter}
}

func cursorFinalizer(c *cursor) {
	c.dbIter.Release()
	c.pendingIter.Release()
}

func (c *cursor) Bucket() database.Bucket {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}
	return c.bucket
}

func (c *cursor) Delete() error {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return err
	}

	if c.currentIter == nil {
		str := "cursor is exhausted"
		return makeError(database.ErrIncompatibleValue, str, nil)
	}

	// Do not allow buckets to be deleted via the cursor.
	key := c.currentIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		str := "buckets may not be deleted from a cursor"
		return makeError(database.ErrIncompatibleValue, str, nil)
	}

	c.bucket.tx.deleteKey(copySlice(key), true)
	return nil
}

func (c *cursor) First() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	c.dbIter.First()
	c.pendingIter.First()
	return c.chooseIterator(true)
}

func (c *cursor) Last() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	c.dbIter.Last()
	c.pendingIter.Last()
	return c.chooseIterator(false)
}

func (c *cursor) Next() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.currentIter == nil {
		return false
	}

	c.currentIter.Next()
	return c.chooseIterator(true)
}

func (c *cursor) Prev() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.currentIter == nil {
		return false
	}

	c.currentIter.Prev()
	return c.chooseIterator(false)
}

func (c *cursor) Seek(seek []byte) bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	seekKey := bucketizedKey(c.bucket.id, seek)
	c.dbIter.Seek(seekKey)
	c.pendingIter.Seek(seekKey)
	return c.chooseIterator(true)
}

func (c *cursor) Key() []byte {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	if c.currentIter == nil {
		return nil
	}

	key := c.currentIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		key = key[len(bucketIndexPrefix)+4:]
		return copySlice(key)
	}

	key = key[len(c.bucket.id):]
	return copySlice(key)
}

func (c *cursor) Value() []byte {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	if c.currentIter == nil {
		return nil
	}

	if bytes.HasPrefix(c.currentIter.Key(), bucketIndexPrefix) {
		return nil
	}

	return copySlice(c.currentIter.Value())
}

func (c *cursor) chooseIterator(forwards bool) bool {
	c.skipPendingUpdates(forwards)

	if !c.dbIter.Valid() && !c.pendingIter.Valid() {
		c.currentIter = nil
		return false
	}

	if !c.pendingIter.Valid() {
		c.currentIter = c.dbIter
		return true
	}

	if !c.dbIter.Valid() {
		c.currentIter = c.pendingIter
		return true
	}

	compare := bytes.Compare(c.dbIter.Key(), c.pendingIter.Key())
	if (forwards && compare > 0) || (!forwards && compare < 0) {
		// case forwards and db iterator key > pending iterator key
		// case backwards and db iterator key < pending iterator key
		c.currentIter = c.pendingIter
	} else {
		c.currentIter = c.dbIter
	}
	return true
}

func (c *cursor) skipPendingUpdates(forwards bool) {
	for c.dbIter.Valid() {
		var skip bool
		key := c.dbIter.Key()
		if c.bucket.tx.pendingRemove.Has(key) {
			skip = true
		} else if c.bucket.tx.pendingKeys.Has(key) {
			skip = true
		}
		if !skip {
			break
		}

		// case pending key. skip and go next/prev.
		if forwards {
			c.dbIter.Next()
		} else {
			c.dbIter.Prev()
		}
	}
}

func (c *cursor) rawKey() []byte {
	if c.currentIter == nil {
		return nil
	}
	return copySlice(c.currentIter.Key())
}

func (c *cursor) rawValue() []byte {
	if c.currentIter == nil {
		return nil
	}
	return copySlice(c.currentIter.Value())
}
