package myqlldb

import (
	"hash/crc32"

	"encoding/hex"

	"encoding/binary"
	"fmt"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/mysqlldb/mysql"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/gocraft/dbr"
)

var (
	// castagnoli houses the Catagnoli polynomial used for CRC-32 checksums.
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

type blockStore struct {
	// network is the specific network to use for each block.
	network wire.BitcoinNet

	readWriteConfig *mysql.Config // mysql config for writable connection.
	readOnlyConfig  *mysql.Config // mysql config for read only connection.

	currentSession *dbr.Session
	currentTx      *dbr.Tx
}

func newBlockStore(
	network wire.BitcoinNet,
	readWriteConfig *mysql.Config,
	readOnlyConfig *mysql.Config,
) *blockStore {
	if readOnlyConfig == nil {
		readOnlyConfig = readWriteConfig
	}
	store := &blockStore{
		network:         network,
		readWriteConfig: readWriteConfig,
		readOnlyConfig:  readOnlyConfig,
	}
	return store
}

func (s *blockStore) readBlock(hash *chainhash.Hash, loc blockLocation) (*btcutil.Block, error) {
	entity, err := s.readBlockByteByLocation(loc)
	if err != nil {
		return nil, err
	}
	return btcutil.NewBlockFromBytes(entity.RawBytes)
}

func (s *blockStore) readBlockByte(hash *chainhash.Hash, loc blockLocation) ([]byte, error) {
	entity, err := s.readBlockByteByLocation(loc)
	if err != nil {
		return nil, err
	}
	return entity.RawBytes, nil
}

func (s *blockStore) readBlockByteByLocation(loc blockLocation) (*mysql.Block, error) {
	store := mysql.NewBlockStore()
	entity, err := store.GetByID(s.currentTx, loc.blockID)
	if err != nil {
		return nil, makeError(database.ErrDriverSpecific, "block data is not saved in mysql database.", err)
	}
	if err != nil {
		return nil, makeError(database.ErrDriverSpecific, "block data is not saved in mysql database.", err)
	}

	checksum, _ := hex.DecodeString(entity.Checksum)
	serializedChecksum := binary.BigEndian.Uint32(checksum)

	var data = make([]byte, 8)
	byteOrder.PutUint32(data[0:4], uint32(s.network))
	byteOrder.PutUint32(data[4:8], entity.BlockLen)
	data = append(data, entity.RawBytes...)
	calculatedChecksum := crc32.Checksum(data, castagnoli)
	if serializedChecksum != calculatedChecksum {
		str := fmt.Sprintf("block data for block %d checksum does not match - got %x, want %x",
			entity.ID, calculatedChecksum, serializedChecksum)
		return nil, makeError(database.ErrCorruption, str, nil)
	}

	if entity.Network != uint32(s.network) {
		str := fmt.Sprintf("block data for block %d is for the wrong network - got %d, want %d",
			entity.ID, entity.Network, uint32(s.network))
		return nil, makeError(database.ErrDriverSpecific, str, nil)
	}

	return entity, nil
}

func (s *blockStore) readBlockRegion(loc blockLocation, offset, numBytes uint32) ([]byte, error) {

	entity, err := s.readBlockByteByLocation(loc)
	if err != nil {
		return nil, err
	}

	if offset < 0 || entity.BlockLen < offset+numBytes {
		str := fmt.Sprintf("block %d region offset %d, length %d exceeds block length of %d",
			entity.ID, offset, numBytes, entity.BlockLen)
		return nil, makeError(database.ErrBlockRegionInvalid, str, nil)
	}

	return entity.RawBytes[offset : offset+numBytes], nil
}

func (s *blockStore) writeBlock(block *btcutil.Block) (blockLocation, error) {

	var err error
	blockStore := mysql.NewBlockStore()
	txOutStore := mysql.NewTransactionOutputStore()
	txOutAddressStore := mysql.NewTransactionOutputAddressStore()

	// block info
	blockBytes, _ := block.Bytes()
	blockLen := uint32(len(blockBytes))
	blockTime := block.MsgBlock().Header.Timestamp
	hash := block.Hash().String()
	height := block.Height()

	hasher := crc32.New(castagnoli)
	var scratch [4]byte
	byteOrder.PutUint32(scratch[:], uint32(s.network))
	_, _ = hasher.Write(scratch[:])
	byteOrder.PutUint32(scratch[:], blockLen)
	_, _ = hasher.Write(scratch[:])
	_, _ = hasher.Write(blockBytes)
	checksum := hex.EncodeToString(hasher.Sum(nil)) // 4bytes -> 8 len hex string.
	blockEntity := mysql.Block{
		Network:   uint32(s.network),
		BlockLen:  blockLen,
		RawBytes:  blockBytes,
		Checksum:  checksum,
		Hash:      hash,
		Height:    height,
		BlockTime: blockTime,
	}
	blockEntity, err = blockStore.Insert(s.currentTx, blockEntity)
	if err != nil {
		return blockLocation{}, makeError(
			database.ErrDriverSpecific, "mysql error on inserting block data.", err)
	}

	// transaction info
	txns := block.Transactions()
	for _, txn := range txns {
		tx := txn.MsgTx()
		txID := txIDFromTx(tx)
		txOuts := tx.TxOut
		for _, txOut := range txOuts {
			amount := txOut.Value // Number of satoshis to spend.
			script := txOut.PkScript
			scriptClass, addresses, _, _ := txscript.ExtractPkScriptAddrs(script, s.networkParams())

			txOutEntity := mysql.TransactionOutput{
				BlockID:       blockEntity.ID,
				TransactionID: txID,
				Amount:        amount,
				PkScriptBytes: script,
				PkScriptClass: int(scriptClass),
			}
			txOutEntity, err = txOutStore.Insert(s.currentTx, txOutEntity)
			if err != nil {
				return blockLocation{}, makeError(
					database.ErrDriverSpecific, "mysql error on inserting tx out data.", err)
			}

			if addresses != nil && 0 < len(addresses) {
				addressEntities := make([]mysql.TransactionOutputAddress, len(addresses))
				for i := range addresses {
					addressEntities[i].BlockID = blockEntity.ID
					addressEntities[i].TxOutID = txOutEntity.ID
					addressEntities[i].Address = addresses[i].EncodeAddress()
				}
				err = txOutAddressStore.BulkInsert(s.currentTx, addressEntities)
				if err != nil {
					return blockLocation{}, makeError(
						database.ErrDriverSpecific, "mysql error on inserting tx out address data.", err)
				}
			}
		}
	}

	return blockLocation{}, nil
}

func (s *blockStore) networkParams() *chaincfg.Params {
	switch s.network {
	case wire.MainNet:
		return &chaincfg.MainNetParams
	case wire.TestNet3:
		return &chaincfg.TestNet3Params
	case wire.TestNet:
		return &chaincfg.RegressionNetParams
	case wire.SimNet:
		return &chaincfg.SimNetParams
	default:
		return nil
	}
}

func (s *blockStore) begin(writable bool) error {

	var config *mysql.Config
	if writable {
		config = s.readWriteConfig
	} else {
		config = s.readOnlyConfig
	}
	session := mysql.NewSession(config)
	s.currentSession = session
	tx, err := session.Begin()
	if err != nil {
		return makeError(database.ErrDriverSpecific, "mysql error on start transaction.", err)
	}
	s.currentTx = tx
	return nil
}

func (s *blockStore) syncBlocks() error {
	err := s.currentTx.Commit()
	if err != nil {
		return makeError(database.ErrDriverSpecific, "mysql error on commit transaction.", err)
	}
	return nil
}

func (s *blockStore) handleRollback() {
	s.currentTx.Rollback()
}

func (s *blockStore) close() error {
	err := s.currentSession.Close()
	if err != nil {
		return makeError(database.ErrDriverSpecific, "mysql error on session close.", err)
	}
	return nil
}

type blockLocation struct {
	blockID uint64
}

func deserializeBlockLoc(serializedLoc []byte) blockLocation {
	return blockLocation{
		blockID: byteOrder.Uint64(serializedLoc[0:8]),
	}
}

func serializeBlockLoc(loc blockLocation) []byte {
	var serializedData [8]byte
	byteOrder.PutUint64(serializedData[0:8], loc.blockID)
	return serializedData[:]
}
