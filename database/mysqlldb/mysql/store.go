package mysql

import (
	"fmt"

	"github.com/gocraft/dbr"
)

type BlockStore struct {
	table string
}

func NewBlockStore() BlockStore {
	return BlockStore{
		table: "blocks",
	}
}

func (s BlockStore) Insert(tx *dbr.Tx, entity Block) (Block, error) {
	sqlRet, err := tx.InsertInto(s.table).Columns(
		"network",
		"block_len",
		"checksum",
		"raw_bytes",
		"hash",
		"height",
		"block_time",
	).Record(entity).Exec()
	if err != nil {
		return entity, err
	}
	id, err := sqlRet.LastInsertId()
	if err != nil {
		return entity, err
	}
	entity.ID = uint64(id)
	return entity, nil
}

func (s BlockStore) GetByID(tx *dbr.Tx, id uint64) (*Block, error) {
	entity := &Block{}
	err := tx.Select("*").
		From(s.table).
		Where("id = ?", id).
		LoadOne(&entity)
	if err != nil {
		return nil, err
	}
	return entity, nil
}

type TransactionOutputStore struct {
	table string
}

func NewTransactionOutputStore() TransactionOutputStore {
	return TransactionOutputStore{
		table: "transaction_outputs",
	}
}

func (s TransactionOutputStore) Insert(tx *dbr.Tx, entity TransactionOutput) (TransactionOutput, error) {
	sqlRet, err := tx.InsertInto(s.table).Columns(
		"block_id",
		"transaction_id",
		"amount",
		"pk_script_bytes",
		"pk_script_class",
	).Record(entity).Exec()
	if err != nil {
		return entity, err
	}
	id, err := sqlRet.LastInsertId()
	if err != nil {
		return entity, err
	}
	entity.ID = uint64(id)
	return entity, nil
}

type TransactionOutputAddressStore struct {
	table string
}

func NewTransactionOutputAddressStore() TransactionOutputAddressStore {
	return TransactionOutputAddressStore{
		table: "transaction_output_addresses",
	}
}

func (s TransactionOutputAddressStore) BulkInsert(tx *dbr.Tx, entities []TransactionOutputAddress) error {

	query := fmt.Sprintf(
		"insert ignore into `%s` (`block_id`, `tx_out_id`, `address`) values ",
		s.table,
	)
	values := []interface{}{}
	for i, ent := range entities {
		if 0 < i {
			query += ","
		}
		query += "(?, ?, ?)"
		values = append(
			values,
			ent.BlockID,
			ent.TxOutID,
			ent.Address,
		)
	}
	_, err := tx.InsertBySql(query, values...).Exec()
	return err
}
