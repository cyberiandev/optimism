package indexer_test

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum-optimism/optimism/go/ctc/indexer"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func makeHeader(parent common.Hash, number uint64) *types.Header {
	return &types.Header{
		ParentHash: parent,
		Number:     new(big.Int).SetUint64(number),
	}
}

type MockBackend struct {
	headersByNumber map[uint64]*types.Header
	headersByHash   map[common.Hash]*types.Header
}

func NewMockBackend() *MockBackend {
	b := &MockBackend{
		headersByNumber: make(map[uint64]*types.Header),
		headersByHash:   make(map[common.Hash]*types.Header),
	}

	genesis := makeHeader(common.Hash{}, 0)
	b.extend(genesis)

	return b
}

func (b *MockBackend) ExtendByHeight(number uint64) *types.Header {
	parent := b.headersByNumber[number-1]
	header := makeHeader(parent.Hash(), number)
	b.extend(header)
	return header
}

func (b *MockBackend) extend(header *types.Header) {
	number := header.Number.Uint64()
	blockHash := header.Hash()
	if (header.ParentHash != common.Hash{}) {
		parent, ok := b.headersByHash[header.ParentHash]
		if !ok {
			panic("header does not connect to chain")
		} else if parent.Number.Uint64()+1 != number {
			panic(fmt.Sprintf("header number %v is not sequential", number))
		}
	} else if number != 0 {
		panic("genesis header must be number 0")
	}

	b.headersByNumber[number] = header
	b.headersByHash[blockHash] = header
}

func (b *MockBackend) HeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error) {
	header, ok := b.headersByHash[hash]
	if !ok {
		return nil, fmt.Errorf("Header hash=%v not found", hash)
	}
	return header, nil
}

func (b *MockBackend) HeaderByNumber(_ context.Context, number *big.Int) (*types.Header, error) {
	header, ok := b.headersByNumber[number.Uint64()]
	if !ok {
		return nil, fmt.Errorf("Header number=%v not found", number)
	}
	return header, nil
}

type forkSelectorTestCase struct {
	cfg indexer.HeaderSelectorConfig

	// startHeight specifies the height of the first block within the testable
	// range. The chain will be initialized with startHeight - 1 blocks before
	// genExp is called.
	startHeight uint64

	// endHeight specifies the height of the last block within the testable
	// range.
	//
	// NOTE: MUST be greater than or equal to startHeight.
	endHeight uint64

	// genExp controls the test behavior for each block number in the range
	// [startHeight, endHeight].
	//
	// If `applyToSelector` is false, the new header will _not_ be passed to
	// NewHead and the other return values are ignored.
	//
	// Otherwise, `lowest` should contain the lowest block height processed from
	// the PoV of the caller, and `expConfNumbers` should contain the expected
	// block numbers of the headers returned from NewHead(_, lowest, header, _).
	//
	// NOTE: The value of `lowest` does not necessarily need to be sequential
	// and/or correspond to the highest block number returned from NewHead. This
	// can be used to simulate behavior when processing a prior block range
	// fully or partially fails, and needs to be retried.
	genExp func(number uint64) (applyToSelector bool, lowest uint64, expConfNumbers []uint64)
}

// TestConfirmedHeaderSelectorBlockLowerThanConfDepth asserts that NewHead does
// not consider headers confirmed when the chain is shorter than ConfDepth.
func TestConfirmedHeaderSelectorBlockLowerThanConfDepth(t *testing.T) {
	testConfirmedHeaderSelector(t, forkSelectorTestCase{
		cfg: indexer.HeaderSelectorConfig{
			ConfDepth:    20,
			MaxBatchSize: 100,
		},
		startHeight: 19,
		endHeight:   19,
		genExp: func(number uint64) (bool, uint64, []uint64) {
			if number == 19 {
				return true, 0, nil
			}
			panic(fmt.Sprintf("unexpected number %v", number))
		},
	})
}

// TestConfirmedHeaderSelectorConfirmBlockOne asserts that block one is
// considered confirmed once the height of the chain reaches ConfDepth.
func TestConfirmedHeaderSelectorConfirmBlockOne(t *testing.T) {
	testConfirmedHeaderSelector(t, forkSelectorTestCase{
		cfg: indexer.HeaderSelectorConfig{
			ConfDepth:    20,
			MaxBatchSize: 100,
		},
		startHeight: 20,
		endHeight:   20,
		genExp: func(number uint64) (bool, uint64, []uint64) {
			if number == 20 {
				return true, 0, []uint64{1}
			}
			panic(fmt.Sprintf("unexpected number %v", number))
		},
	})
}

// TestConfirmedHeaderSelectionConfirmBlockOneWithOneConf is the same as
// TestConfirmedHeaderSelectorConfirmBlockOne, but uses a ConfDepth of 1 to test
// for off-by-one arithmetic errors.
func TestConfirmedHeaderSelectorConfirmBlockOneWithOneConf(t *testing.T) {
	testConfirmedHeaderSelector(t, forkSelectorTestCase{
		cfg: indexer.HeaderSelectorConfig{
			ConfDepth:    1,
			MaxBatchSize: 100,
		},
		startHeight: 1,
		endHeight:   1,
		genExp: func(number uint64) (bool, uint64, []uint64) {
			if number == 1 {
				return true, 0, []uint64{1}
			}
			panic(fmt.Sprintf("unexpected number %v", number))
		},
	})
}

// TestConfirmedHeaderSelectorConfirmSuccessiveBlock asserts that NewHead
// returns headers with sequential block numbers when lowest is set to the
// highest block number of a previous return from NewHead. This simualtes the
// happy path when processing blocks at tip.
func TestConfirmedHeaderSelectorConfirmSuccessiveBlock(t *testing.T) {
	testConfirmedHeaderSelector(t, forkSelectorTestCase{
		cfg: indexer.HeaderSelectorConfig{
			ConfDepth:    20,
			MaxBatchSize: 100,
		},
		startHeight: 20,
		endHeight:   21,
		genExp: func(number uint64) (bool, uint64, []uint64) {
			switch number {
			case 20:
				return true, 0, []uint64{1}
			case 21:
				return true, 1, []uint64{2}
			default:
				panic(fmt.Sprintf("unexpected number %v", number))
			}
		},
	})
}

// TestConfirmedHeaderSelectorSkippedBlock asserts that NewHead returns the
// relevant headers if a call to NewHead is skipped for a particular block.
// For example, if SubscribeNewHead were to drop a notification, this asserts
// that we can recover.
func TestConfirmedHeaderSelectorSkippedBlock(t *testing.T) {
	testConfirmedHeaderSelector(t, forkSelectorTestCase{
		cfg: indexer.HeaderSelectorConfig{
			ConfDepth:    20,
			MaxBatchSize: 100,
		},
		startHeight: 20,
		endHeight:   22,
		genExp: func(number uint64) (bool, uint64, []uint64) {
			switch number {
			case 20:
				return true, 0, []uint64{1}
			case 21:
				return false, 0, nil
			case 22:
				return true, 1, []uint64{2, 3}
			default:
				panic(fmt.Sprintf("unexpected number %v", number))
			}
		},
	})
}

// TestConfirmedHeaderSelectorInitialSync asserts that NewHead returns all
// missing headers when lowest is lagging behind the new found block.
func TestConfirmedHeaderSelectorInitialSync(t *testing.T) {
	testConfirmedHeaderSelector(t, forkSelectorTestCase{
		cfg: indexer.HeaderSelectorConfig{
			ConfDepth:    20,
			MaxBatchSize: 100,
		},
		startHeight: 119,
		endHeight:   119,
		genExp: func(number uint64) (bool, uint64, []uint64) {
			switch number {
			case 119:
				return true, 0, makeBlockNumbers(1, 100) // 119 - (20 - 1)
			default:
				panic(fmt.Sprintf("unexpected number %v", number))
			}
		},
	})
}

// TestConfirmedHeaderSelectorInitialSyncDeeperThanMaxBatchSize asserts that
// NewHead clamps the returned headers to the MaxBatchSize.
func TestConfirmedHeaderSelectorInitialSyncDeeperThanMaxBatchSize(t *testing.T) {
	testConfirmedHeaderSelector(t, forkSelectorTestCase{
		cfg: indexer.HeaderSelectorConfig{
			ConfDepth:    20,
			MaxBatchSize: 100,
		},
		startHeight: 120,
		endHeight:   120,
		genExp: func(number uint64) (bool, uint64, []uint64) {
			switch number {
			case 120:
				return true, 0, makeBlockNumbers(1, 100)
			default:
				panic(fmt.Sprintf("unexpected number %v", number))
			}
		},
	})
}

func testConfirmedHeaderSelector(t *testing.T, test forkSelectorTestCase) {
	ctx := context.Background()

	if test.startHeight > test.endHeight {
		panic(fmt.Sprintf("malformed test, startHeight=%d > endHeight=%d",
			test.startHeight, test.endHeight))
	}

	// Prepopulate the backend with startHeight - 1 blocks.
	mockBackend := NewMockBackend()
	for number := uint64(1); number < test.startHeight; number++ {
		_ = mockBackend.ExtendByHeight(number)
	}

	forkSelector := indexer.NewConfirmedHeaderSelector(test.cfg)

	// Create new blocks for each height in the test range.
	for number := test.startHeight; number <= test.endHeight; number++ {
		header := mockBackend.ExtendByHeight(number)

		// Deteremine if the tests specifies to pass the new header to NewHead.
		// If so, assert that the returned, confirmed headers contain the
		// expected block numbers.
		applyToSelector, lowest, expConfNumbers := test.genExp(number)
		if applyToSelector {
			confHeaders := forkSelector.NewHead(ctx, lowest, header, mockBackend)
			confNumbers := extractBlockNumbers(confHeaders)
			assertNumberEqual(t, expConfNumbers, confNumbers)
		}
	}
}

func makeBlockNumbers(start, end uint64) []uint64 {
	var numbers []uint64
	for i := start; i <= end; i++ {
		numbers = append(numbers, i)
	}
	return numbers
}

func extractBlockNumbers(headers []*types.Header) []uint64 {
	var numbers []uint64
	for _, header := range headers {
		numbers = append(numbers, header.Number.Uint64())
	}
	return numbers
}

func assertNumberEqual(t *testing.T, expConfNumbers []uint64, confNumbers []uint64) {
	if !reflect.DeepEqual(expConfNumbers, confNumbers) {
		t.Fatalf("conf header numbers mismatch\n  want: %v\n  got: %v",
			expConfNumbers, confNumbers)
	}
}
