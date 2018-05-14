package myqlldb

import (
	"github.com/btcsuite/btcd/database/internal/treap"
	"github.com/btcsuite/goleveldb/leveldb/util"
)

type ldbTreapIter struct {
	*treap.Iterator
	tx       *tx
	released bool
}

func (i *ldbTreapIter) Error() error {
	return nil
}

func (i *ldbTreapIter) SetReleaser(releaser util.Releaser) {
}

func (iter *ldbTreapIter) Release() {
	if !iter.released {
		iter.tx.removeActiveIter(iter.Iterator)
		iter.released = true
	}
}

func newLdbTreapIter(tx *tx, slice *util.Range) *ldbTreapIter {
	iter := tx.pendingKeys.Iterator(slice.Start, slice.Limit)
	tx.addActiveIter(iter)
	return &ldbTreapIter{Iterator: iter, tx: tx}
}
