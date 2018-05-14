package myqlldb

import (
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/goleveldb/leveldb"
	ldbErrors "github.com/btcsuite/goleveldb/leveldb/errors"
)

// Common error strings.
const (
	// errDbNotOpenStr is the text to use for the database.ErrDbNotOpen error code.
	errDbNotOpenStr = "database is not open"

	// errTxClosedStr is the text to use for the database.ErrTxClosed error code.
	errTxClosedStr = "database tx is closed"
)

func makeError(code database.ErrorCode, desc string, err error) database.Error {
	return database.Error{ErrorCode: code, Description: desc, Err: err}
}

// convertErr converts the passed leveldb error into a database error with an
// equivalent error code  and the passed description.  It also sets the passed
// error as the underlying error.
func convertErr(desc string, ldbErr error) database.Error {
	var code database.ErrorCode
	switch {
	case ldbErrors.IsCorrupted(ldbErr): // Database corruption errors.
		code = database.ErrCorruption
	case ldbErr == leveldb.ErrClosed: // Database open/create errors.
		code = database.ErrDbNotOpen
	case ldbErr == leveldb.ErrSnapshotReleased: // Transaction errors.
		code = database.ErrTxClosed
	case ldbErr == leveldb.ErrIterReleased: // Transaction errors.
		code = database.ErrTxClosed
	default:
		code = database.ErrDriverSpecific
	}
	return makeError(code, desc, ldbErr)
}
