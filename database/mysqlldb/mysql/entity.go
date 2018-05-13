package mysql

import "time"

type Block struct {
	ID        uint64    `db:"id"`
	Network   uint32    `db:"network"`
	BlockLen  uint32    `db:"block_len"`
	Checksum  string    `db:"checksum"`
	RawBytes  []byte    `db:"raw_bytes"`
	Hash      string    `db:"hash"`
	Height    int32     `db:"height"`
	BlockTime time.Time `db:"block_time"`
}

type TransactionOutput struct {
	ID            uint64 `db:"id"`
	BlockID       uint64 `db:"block_id"`
	TransactionID string `db:"transaction_id"`
	Amount        int64  `db:"amount"`
	PkScriptBytes []byte `db:"pk_script_bytes"`
	PkScriptClass int    `db:"pk_script_class"`
}

type TransactionOutputAddress struct {
	ID      uint64 `db:"id"`
	BlockID uint64 `db:"block_id"`
	TxOutID uint64 `db:"tx_out_id"`
	Address string `db:"address"`
}
