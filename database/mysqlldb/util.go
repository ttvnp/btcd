package myqlldb

import (
	"os"

	"bytes"

	"crypto/sha256"

	"encoding/hex"

	"github.com/btcsuite/btcd/wire"
)

// copySlice returns a copy of the passed slice.  This is mostly used to copy
// leveldb iterator keys and values since they are only valid until the iterator
// is moved instead of during the entirety of the transaction.
func copySlice(slice []byte) []byte {
	ret := make([]byte, len(slice))
	copy(ret, slice)
	return ret
}

// bucketIndexKey returns the actual key to use for storing and retrieving a
// child bucket in the bucket index.  This is required because additional
// information is needed to distinguish nested buckets with the same name.
func bucketIndexKey(parentID [4]byte, key []byte) []byte {
	// The serialized bucket index key format is:
	//   <bucketindexprefix><parentbucketid><bucketname>
	indexKey := make([]byte, len(bucketIndexPrefix)+4+len(key))
	copy(indexKey, bucketIndexPrefix)
	copy(indexKey[len(bucketIndexPrefix):], parentID[:])
	copy(indexKey[len(bucketIndexPrefix)+4:], key)
	return indexKey
}

// bucketizedKey returns the actual key to use for storing and retrieving a key
// for the provided bucket ID.  This is required because bucketizing is handled
// through the use of a unique prefix per bucket.
func bucketizedKey(bucketID [4]byte, key []byte) []byte {
	// The serialized block index key format is:
	//   <bucketid><key>
	bKey := make([]byte, 4+len(key))
	copy(bKey, bucketID[:])
	copy(bKey[4:], key)
	return bKey
}

// bulkFetchData is allows a block location to be specified along with the
// index it was requested from.  This in turn allows the bulk data loading
// functions to sort the data accesses based on the location to improve
// performance while keeping track of which result the data is for.
type bulkFetchData struct {
	*blockLocation
	replyIndex int
}

// bulkFetchDataSorter implements sort.Interface to allow a slice of
// bulkFetchData to be sorted.  In particular it sorts by file and then
// offset so that reads from files are grouped and linear.
type bulkFetchDataSorter []bulkFetchData

// Len returns the number of items in the slice.  It is part of the
// sort.Interface implementation.
func (s bulkFetchDataSorter) Len() int {
	return len(s)
}

// Swap swaps the items at the passed indices.  It is part of the
// sort.Interface implementation.
func (s bulkFetchDataSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less returns whether the item with index i should sort before the item with
// index j.  It is part of the sort.Interface implementation.
func (s bulkFetchDataSorter) Less(i, j int) bool {
	return s[i].blockID < s[j].blockID
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func txToBytes(tx *wire.MsgTx) []byte {
	w := bytes.NewBuffer(make([]byte, 0, tx.SerializeSize()))
	err := tx.Serialize(w)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func txIDFromTx(tx *wire.MsgTx) string {
	once := sha256.Sum256(txToBytes(tx))
	double := sha256.Sum256(once[:])
	return hex.EncodeToString(double[:])
}
