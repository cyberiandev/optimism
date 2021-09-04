package indexer

import (
	"database/sql"
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	_ "github.com/mattn/go-sqlite3"
)

var createBlocksTable = `
CREATE TABLE IF NOT EXISTS blocks (
	hash TEXT NOT NULL PRIMARY KEY,
	parent_hash TEXT NOT NULL,
	number INTEGER NOT NULL,
	timestamp INTEGER NOT NULL
)
`

var createDepositsTable = `
CREATE TABLE IF NOT EXISTS deposits (
	queue_index INTEGER NOT NULL UNIQUE,
	tx_hash TEXT NOT NULL,
	block_hash TEXT NOT NULL REFERENCES blocks(hash) ,
	l1_tx_origin TEXT NOT NULL,
	target TEXT NOT NULL,
	gas_limit TEXT NOT NULL,
	data BLOB NOT NULL
)
`

var schema = []string{
	createBlocksTable,
	createDepositsTable,
}

type TxnEnqueuedEvent struct {
	BlockNumber uint64
	Timestamp   uint64
	QueueIndex  uint64
	TxHash      common.Hash
	L1TxOrigin  common.Address
	Target      common.Address
	GasLimit    *big.Int
	Data        []byte
}

func (e TxnEnqueuedEvent) String() string {
	return fmt.Sprintf("TxnEnqueuedEvent { BlockNumber: %d, Timestamp: %d, "+
		"QueueIndex: %d, TxHash: %s, L1TxOrigin: %s, Target: %s, "+
		"GasLimit: %s, Data: %x }", e.BlockNumber, e.Timestamp, e.QueueIndex,
		e.TxHash, e.L1TxOrigin, e.Target, e.GasLimit, e.Data)
}

type IndexedBlock struct {
	Hash       common.Hash
	ParentHash common.Hash
	Number     uint64
	Timestamp  uint64
	Deposits   []Deposit
}

func (b IndexedBlock) String() string {
	return fmt.Sprintf("IndexedBlock { Hash: %s, ParentHash: %s, Number: %d, "+
		"Timestamp: %d, Deposits: %s }", b.Hash, b.ParentHash, b.Number,
		b.Timestamp, b.Deposits)
}

type Deposit struct {
	QueueIndex uint64
	TxHash     common.Hash
	L1TxOrigin common.Address
	Target     common.Address
	GasLimit   *big.Int
	Data       []byte
}

func (d Deposit) String() string {
	return fmt.Sprintf("Deposit { QueueIndex: %d, TxHash: %s, L1TxOrigin: %s, "+
		"Target: %s, GasLimit: %s, Data: %x }", d.QueueIndex, d.TxHash,
		d.L1TxOrigin, d.Target, d.GasLimit, d.Data)
}

func (b *IndexedBlock) Events() []TxnEnqueuedEvent {
	nDeposits := len(b.Deposits)
	if nDeposits == 0 {
		return nil
	}

	var events = make([]TxnEnqueuedEvent, 0, nDeposits)
	for _, deposit := range b.Deposits {
		events = append(events, TxnEnqueuedEvent{
			BlockNumber: b.Number,
			Timestamp:   b.Timestamp,
			QueueIndex:  deposit.QueueIndex,
			TxHash:      deposit.TxHash,
			L1TxOrigin:  deposit.L1TxOrigin,
			Target:      deposit.Target,
			GasLimit:    deposit.GasLimit,
			Data:        deposit.Data, // TODO: copy?
		})
	}

	// Ensure that the events are always sorted by increasing queue index.
	sort.Slice(events, func(i, j int) bool {
		return events[i].QueueIndex < events[j].QueueIndex
	})

	return events
}

type Database struct {
	db   *sql.DB
	path string
}

func (d *Database) AddIndexedBlock(block *IndexedBlock) error {
	const insertBlockStatement = `
	INSERT INTO blocks
		(hash, parent_hash, number, timestamp)
	VALUES
		($1, $2, $3, $4)
	`

	const insertDepositStatement = `
	INSERT INTO deposits
		(queue_index, tx_hash, block_hash, l1_tx_origin, target, gas_limit, data)
	VALUES
		($1, $2, $3, $4, $5, $6, $7)
	`

	return txn(d.db, func(tx *sql.Tx) error {
		blockStmt, err := tx.Prepare(insertBlockStatement)
		if err != nil {
			return err
		}

		_, err = blockStmt.Exec(
			block.Hash.String(),
			block.ParentHash.String(),
			block.Number,
			block.Timestamp,
		)
		if err != nil {
			return err
		}

		if len(block.Deposits) == 0 {
			return nil
		}

		depositStmt, err := tx.Prepare(insertDepositStatement)
		if err != nil {
			return err
		}

		for _, deposit := range block.Deposits {
			_, err = depositStmt.Exec(
				deposit.QueueIndex,
				deposit.TxHash.String(),
				block.Hash.String(),
				deposit.L1TxOrigin.String(),
				deposit.Target.String(),
				deposit.GasLimit.String(),
				deposit.Data,
			)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

type BlockLocator struct {
	Number uint64
	Hash   common.Hash
}

func (d *Database) GetHighestBlock() (*BlockLocator, error) {
	const selectHighestBlockStatement = `
	SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 1
	`

	var highestBlock *BlockLocator
	err := txn(d.db, func(tx *sql.Tx) error {
		queryStmt, err := tx.Prepare(selectHighestBlockStatement)
		if err != nil {
			return err
		}

		rows, err := queryStmt.Query()
		if err != nil {
			return err
		}

		if !rows.Next() {
			return nil
		}

		var number uint64
		var hash string
		err = rows.Scan(&number, &hash)
		if err != nil {
			return err
		}

		if rows.Next() {
			panic("number of rows should be at most 1 since LIMIT is 1")
		}

		highestBlock = &BlockLocator{
			Number: number,
			Hash:   common.HexToHash(hash),
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return highestBlock, nil
}

func (d *Database) GetIndexedBlockByHash(hash common.Hash) (*IndexedBlock, error) {
	const selectBlockByHashStatement = `
	SELECT
		hash, parent_hash, number, timestamp
	FROM blocks
	WHERE hash = $1
	`

	var block *IndexedBlock
	err := txn(d.db, func(tx *sql.Tx) error {
		queryStmt, err := tx.Prepare(selectBlockByHashStatement)
		if err != nil {
			return err
		}

		rows, err := queryStmt.Query(hash.String())
		if err != nil {
			return err
		}

		if !rows.Next() {
			return nil
		}

		var hash string
		var parentHash string
		var number uint64
		var timestamp uint64
		err = rows.Scan(&hash, &parentHash, &number, &timestamp)
		if err != nil {
			return err
		}

		block = &IndexedBlock{
			Hash:       common.HexToHash(hash),
			ParentHash: common.HexToHash(parentHash),
			Number:     number,
			Timestamp:  timestamp,
			Deposits:   nil,
		}

		if rows.Next() {
			panic("number of rows should be at most 1 since hash is pk")
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return block, nil

}
func (d *Database) GetEventByQueueIndex(queueIndex uint64) (*TxnEnqueuedEvent, error) {
	const selectEventByQueueIndex = `
	SELECT
		b.number, b.timestamp,
		d.queue_index, d.tx_hash, d.l1_tx_origin, d.target, d.gas_limit, d.data
	FROM
		blocks AS b,
		deposits AS d
	WHERE b.hash = d.block_hash AND d.queue_index = $1
	`

	var event *TxnEnqueuedEvent
	err := txn(d.db, func(tx *sql.Tx) error {
		queryStmt, err := tx.Prepare(selectEventByQueueIndex)
		if err != nil {
			return err
		}

		rows, err := queryStmt.Query(queueIndex)
		if err != nil {
			return err
		}

		if !rows.Next() {
			return nil
		}

		e, err := scanTxnEnqueuedEvent(rows)
		if err != nil {
			return err
		}

		if rows.Next() {
			panic("number of rows should be at most 1 since queue_index is pk")
		}

		event = &e

		return nil
	})
	if err != nil {
		return nil, err
	}

	return event, nil
}

func (d *Database) GetEventsByBlockHash(hash common.Hash) ([]TxnEnqueuedEvent, error) {
	const selectEventsByBlockHashStatement = `
	SELECT
		b.number, b.timestamp,
		d.queue_index, d.tx_hash, d.l1_tx_origin, d.target, d.gas_limit, d.data
	FROM
		blocks AS b,
		deposits AS d
	WHERE b.hash = d.block_hash AND b.hash = $1
	ORDER BY d.queue_index
	`

	var events []TxnEnqueuedEvent
	err := txn(d.db, func(tx *sql.Tx) error {
		queryStmt, err := tx.Prepare(selectEventsByBlockHashStatement)
		if err != nil {
			return err
		}

		rows, err := queryStmt.Query(hash.String())
		if err != nil {
			return err
		}

		for rows.Next() {
			event, err := scanTxnEnqueuedEvent(rows)
			if err != nil {
				return err
			}

			events = append(events, event)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return events, nil
}

func scanTxnEnqueuedEvent(rows *sql.Rows) (TxnEnqueuedEvent, error) {
	var number uint64
	var timestamp uint64
	var queueIndex uint64
	var txHash string
	var l1TxOrigin string
	var target string
	var gasLimitStr string
	var data []byte
	err := rows.Scan(
		&number,
		&timestamp,
		&queueIndex,
		&txHash,
		&l1TxOrigin,
		&target,
		&gasLimitStr,
		&data,
	)
	if err != nil {
		return TxnEnqueuedEvent{}, err
	}

	gasLimit, ok := new(big.Int).SetString(gasLimitStr, 10)
	if !ok {
		panic(fmt.Sprintf("Invalid gasLimit string \"%v\"", gasLimitStr))
	}

	return TxnEnqueuedEvent{
		BlockNumber: number,
		Timestamp:   timestamp,
		QueueIndex:  queueIndex,
		TxHash:      common.HexToHash(txHash),
		L1TxOrigin:  common.HexToAddress(l1TxOrigin),
		Target:      common.HexToAddress(target),
		GasLimit:    gasLimit,
		Data:        data,
	}, nil
}

func (d *Database) Path() string {
	return d.path
}

func (d *Database) Close() error {
	return d.db.Close()
}

func NewDatabase(path string) (*Database, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	for _, migration := range schema {
		_, err = db.Exec(migration)
		if err != nil {
			return nil, err
		}
	}

	return &Database{
		db:   db,
		path: path,
	}, nil
}

func txn(db *sql.DB, apply func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	err = apply(tx)
	if err != nil {
		// Don't swallow application error
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}
