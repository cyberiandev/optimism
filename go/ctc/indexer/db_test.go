package indexer_test

import (
	"bytes"
	"io/ioutil"
	"math/big"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/ethereum-optimism/optimism/go/ctc/indexer"
	"github.com/ethereum/go-ethereum/common"
)

func tempDbFile(t *testing.T) *os.File {
	t.Helper()

	file, err := ioutil.TempFile("", "indexerdb")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	return file
}

func createDb(t *testing.T) *indexer.Database {
	t.Helper()

	file := tempDbFile(t)
	db, err := indexer.NewDatabase(file.Name())
	if err != nil {
		t.Fatalf("unable to create database: %v", err)
	}
	return db
}

func openDbPath(t *testing.T, path string) *indexer.Database {
	t.Helper()

	db, err := indexer.NewDatabase(path)
	if err != nil {
		t.Fatalf("unable to create database: %v", err)
	}
	return db
}

func closeDb(t *testing.T, db *indexer.Database) {
	t.Helper()

	if err := db.Close(); err != nil {
		t.Fatalf("unable to close database: %v", err)
	}
}

func cleanupDb(t *testing.T, db *indexer.Database) {
	t.Helper()

	closeDb(t, db)
	os.Remove(db.Path())
}

func TestDatabaseCreate(t *testing.T) {
	file := tempDbFile(t)
	defer os.Remove(file.Name())

	db := openDbPath(t, file.Name())
	closeDb(t, db)
}

func TestDatabaseReopen(t *testing.T) {
	file := tempDbFile(t)
	defer os.Remove(file.Name())

	db := openDbPath(t, file.Name())
	closeDb(t, db)

	db = openDbPath(t, file.Name())
	closeDb(t, db)
}

func TestDatabaseAddIndexedBlockNoDeposits(t *testing.T) {
	testDatabaseAddIndexedBlock(t, &indexer.IndexedBlock{
		Hash:       common.BytesToHash(bytes.Repeat([]byte{0x01}, 32)),
		ParentHash: common.BytesToHash(bytes.Repeat([]byte{0x02}, 32)),
		Number:     5,
		Timestamp:  1600000,
		Deposits:   nil,
	})
}

func TestDatabaseAddIndexedBlockOneDeposit(t *testing.T) {
	testDatabaseAddIndexedBlock(t, &indexer.IndexedBlock{
		Hash:       common.BytesToHash(bytes.Repeat([]byte{0x01}, 32)),
		ParentHash: common.BytesToHash(bytes.Repeat([]byte{0x02}, 32)),
		Number:     5,
		Timestamp:  1600000,
		Deposits: []indexer.Deposit{
			{
				TxHash:     common.BytesToHash(bytes.Repeat([]byte{0xaa}, 32)),
				QueueIndex: 30,
				L1TxOrigin: common.BytesToAddress(bytes.Repeat([]byte{0x03}, 20)),
				Target:     common.BytesToAddress(bytes.Repeat([]byte{0x04}, 20)),
				GasLimit:   new(big.Int).SetUint64(23468273),
				Data:       bytes.Repeat([]byte{0x05}, 42),
			},
		},
	})
}

func TestDatabaseAddIndexedBlockMultipleDeposit(t *testing.T) {
	testDatabaseAddIndexedBlock(t, &indexer.IndexedBlock{
		Hash:       common.BytesToHash(bytes.Repeat([]byte{0x01}, 32)),
		ParentHash: common.BytesToHash(bytes.Repeat([]byte{0x02}, 32)),
		Number:     5,
		Timestamp:  1600000,
		Deposits: []indexer.Deposit{
			{
				TxHash:     common.BytesToHash(bytes.Repeat([]byte{0xaa}, 32)),
				QueueIndex: 30,
				L1TxOrigin: common.BytesToAddress(bytes.Repeat([]byte{0x03}, 20)),
				Target:     common.BytesToAddress(bytes.Repeat([]byte{0x04}, 20)),
				GasLimit:   new(big.Int).SetUint64(23468273),
				Data:       bytes.Repeat([]byte{0x05}, 42),
			},
			{
				TxHash:     common.BytesToHash(bytes.Repeat([]byte{0xbb}, 32)),
				QueueIndex: 31,
				L1TxOrigin: common.BytesToAddress(bytes.Repeat([]byte{0x06}, 20)),
				Target:     common.BytesToAddress(bytes.Repeat([]byte{0x07}, 20)),
				GasLimit:   new(big.Int).SetUint64(23468274),
				Data:       bytes.Repeat([]byte{0x08}, 42),
			},
		},
	})
}

// TestDatabaseAddIndexedBlockMultipleDepositSortedByQueueIndex asserts that
// the database always returns events from a particular block sorted by
// increasing sequence number, even if they are inserted out of order.
func TestDatabaseAddIndexedBlockMultipleDepositSortedByQueueIndex(t *testing.T) {
	testDatabaseAddIndexedBlock(t, &indexer.IndexedBlock{
		Hash:       common.BytesToHash(bytes.Repeat([]byte{0x01}, 32)),
		ParentHash: common.BytesToHash(bytes.Repeat([]byte{0x02}, 32)),
		Number:     5,
		Timestamp:  1600000,
		Deposits: []indexer.Deposit{
			{
				TxHash:     common.BytesToHash(bytes.Repeat([]byte{0xbb}, 32)),
				QueueIndex: 31,
				L1TxOrigin: common.BytesToAddress(bytes.Repeat([]byte{0x06}, 20)),
				Target:     common.BytesToAddress(bytes.Repeat([]byte{0x07}, 20)),
				GasLimit:   new(big.Int).SetUint64(23468274),
				Data:       bytes.Repeat([]byte{0x08}, 42),
			},
			{
				TxHash:     common.BytesToHash(bytes.Repeat([]byte{0xaa}, 32)),
				QueueIndex: 30,
				L1TxOrigin: common.BytesToAddress(bytes.Repeat([]byte{0x03}, 20)),
				Target:     common.BytesToAddress(bytes.Repeat([]byte{0x04}, 20)),
				GasLimit:   new(big.Int).SetUint64(23468273),
				Data:       bytes.Repeat([]byte{0x05}, 42),
			},
		},
	})
}

func testDatabaseAddIndexedBlock(t *testing.T, block *indexer.IndexedBlock) {
	db := createDb(t)
	defer cleanupDb(t, db)

	err := db.AddIndexedBlock(block)
	if err != nil {
		t.Fatalf("Unable to ingest indexed block: %v", err)
	}

	events, err := db.GetEventsByBlockHash(block.Hash)
	if err != nil {
		t.Fatalf("Unable to get events: %v", err)
	}

	// Events should returns the sorted events.
	expEvents := block.Events()
	if !sort.SliceIsSorted(expEvents, func(i, j int) bool {
		return expEvents[i].QueueIndex < expEvents[j].QueueIndex
	}) {
		t.Fatalf("IndexedBlock.Events() returned unsorted events")
	}

	if !reflect.DeepEqual(expEvents, events) {
		t.Fatalf("Events mismatch:\n  want: %v\n  got:  %v", expEvents, events)
	}
}
