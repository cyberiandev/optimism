package indexer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum-optimism/optimism/go/ctc/bindings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
)

const DefaultConnectionTimeout = 5 * time.Second

// errNoChainID represents the error when the chain id is not provided
// and it cannot be remotely fetched
var errNoChainID = errors.New("no chain id provided")

// errWrongChainID represents the error when the configured chain id is not
// correct
var errWrongChainID = errors.New("wrong chain id provided")

type Backend interface {
	bind.ContractBackend
	HeaderBackend

	SubscribeNewHead(context.Context, chan<- *types.Header) (ethereum.Subscription, error)
}

type Indexer struct {
	ctx            context.Context
	stop           chan struct{}
	contract       *bindings.CanonicalTransactionChainFilterer
	backend        Backend
	headerSelector HeaderSelector
	database       *Database
	cfg            *Config
}

func (i *Indexer) Start() error {
	if i.cfg.ChainID == nil {
		return errNoChainID
	}

	go i.Loop()

	return nil
}

func (i *Indexer) Stop() {
	close(i.stop)
}

func (i *Indexer) Wait() {
	<-i.stop
}

func (i *Indexer) Loop() {
	newHeads := make(chan *types.Header, 1000)
	subscription, err := i.backend.SubscribeNewHead(i.ctx, newHeads)
	if err != nil {
		panic(fmt.Sprintf("Unable to subscribe to new heads: %v", err))
	}
	defer subscription.Unsubscribe()

	start := uint64(0)
	for {
		select {
		case header := <-newHeads:
			log.Info("Received new header", header)
			for {
				err := i.Update(start, header)
				if err != nil {
					if err != errNoNewBlocks {
						fmt.Printf("Unable to update indexer: %v", err)
					}
					break
				}
			}

		case <-i.stop:
			return
		}
	}
}

func (i *Indexer) fetchBlockEventIterator(start, end uint64) (
	*bindings.CanonicalTransactionChainTransactionEnqueuedIterator, error) {

	const NUM_RETRIES = 5

	var err error
	for retry := 0; retry < NUM_RETRIES; retry++ {
		ctxt, cancel := context.WithTimeout(i.ctx, DefaultConnectionTimeout)
		defer cancel()

		var iter *bindings.CanonicalTransactionChainTransactionEnqueuedIterator
		iter, err = i.contract.FilterTransactionEnqueued(&bind.FilterOpts{
			Start:   start,
			End:     &end,
			Context: ctxt,
		})
		if err != nil {
			fmt.Printf("    ❌ Unable to query events for block range start=%d end=%d err=%v\n",
				start, end, err)
			continue
		}

		return iter, nil
	}

	return nil, err
}

var errNoNewBlocks = errors.New("no new blocks")

func (i *Indexer) Update(start uint64, newHeader *types.Header) error {
	// var lowest = uint64(13142685) -- 9/1
	// var lowest = uint64(13144701) -- 2 deposits in txn
	var lowest = BlockLocator{
		Number: 13144600,
		Hash:   common.HexToHash("0x3fcc237ed62939dcd97043f43dc83ffeb1033d4e119cf6cc4e009b391237e4ba"),
	}
	highestConfirmed, err := i.database.GetHighestBlock()
	if err != nil {
		return err
	}
	if highestConfirmed != nil {
		lowest = *highestConfirmed
	}

	headers := i.headerSelector.NewHead(i.ctx, lowest.Number, newHeader, i.backend)
	if len(headers) == 0 {
		return errNoNewBlocks
	}

	if lowest.Number+1 != headers[0].Number.Uint64() {
		fmt.Printf("    ⚠️  Block number of block=%d hash=%s does not "+
			"immediately follow lowest block=%d hash=%s",
			headers[0].Number.Uint64(), headers[0].Hash(),
			lowest.Number, lowest.Hash)
		return nil
	}

	if lowest.Hash != headers[0].ParentHash {
		fmt.Printf("    ⚠️  Parent hash of block=%d hash=%s does not "+
			"connect to lowest block=%d hash=%s", headers[0].Number.Uint64(),
			headers[0].Hash(), lowest.Number, lowest.Hash)
		return nil
	}

	startHeight := headers[0].Number.Uint64()
	endHeight := headers[len(headers)-1].Number.Uint64()

	iter, err := i.fetchBlockEventIterator(startHeight, endHeight)
	if err != nil {
		return err
	}

	depositsByBlockHash := make(map[common.Hash][]Deposit)
	for iter.Next() {
		depositsByBlockHash[iter.Event.Raw.BlockHash] = append(
			depositsByBlockHash[iter.Event.Raw.BlockHash],
			Deposit{
				QueueIndex: iter.Event.QueueIndex.Uint64(),
				TxHash:     iter.Event.Raw.TxHash,
				L1TxOrigin: iter.Event.L1TxOrigin,
				Target:     iter.Event.Target,
				GasLimit:   iter.Event.GasLimit,
				Data:       iter.Event.Data,
			},
		)
	}
	if err := iter.Error(); err != nil {
		return err
	}

	for _, header := range headers {
		blockHash := header.Hash()
		number := header.Number.Uint64()
		deposits := depositsByBlockHash[blockHash]

		block := &IndexedBlock{
			Hash:       blockHash,
			ParentHash: header.ParentHash,
			Number:     number,
			Timestamp:  header.Time,
			Deposits:   deposits,
		}

		err := i.database.AddIndexedBlock(block)
		if err != nil {
			fmt.Printf("    ❌ Unable to import block=%d hash=%s err=%v\n"+
				"block: %v\n", number, blockHash, err, block)
			return err
		}

		fmt.Printf("    💾 Imported block=%d hash=%s with %d deposits\n",
			number, blockHash, len(block.Deposits))
		for _, deposit := range block.Deposits {
			fmt.Printf("        💰 Deposit: l1_tx_origin=%s target=%s "+
				"gas_limit=%d queue_index=%d\n", deposit.L1TxOrigin,
				deposit.Target, deposit.GasLimit, deposit.QueueIndex)
		}
	}

	lastHeaderNumber := headers[len(headers)-1].Number.Uint64()
	newHeaderNumber := newHeader.Number.Uint64()
	if lastHeaderNumber+i.cfg.ConfDepth-1 == newHeaderNumber {
		return errNoNewBlocks
	}

	return nil
}

func NewIndexer(cfg *Config) (*Indexer, error) {
	database, err := NewDatabase("indexer.sqlite")
	if err != nil {
		return nil, err
	}

	client, err := ethclient.Dial(cfg.EthereumHttpUrl)
	if err != nil {
		return nil, err
	}

	t := time.NewTicker(5 * time.Second)
	for ; true; <-t.C {
		_, err := client.ChainID(context.Background())
		if err == nil {
			t.Stop()
			break
		}
		log.Error("Unable to connect to remote node", "add", cfg.EthereumHttpUrl)
	}

	address := cfg.CtcAddress
	contract, err := bindings.NewCanonicalTransactionChainFilterer(address, client)
	if err != nil {
		return nil, err
	}

	// Handle restart logic

	log.Info("Creating CTC Indexer")

	chainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, err
	}

	if cfg.ChainID != nil {
		if cfg.ChainID.Cmp(chainID) != 0 {
			return nil, fmt.Errorf("%w: configured with %d and got %d",
				errWrongChainID, cfg.ChainID, chainID)
		}
	} else {
		cfg.ChainID = chainID
	}

	return &Indexer{
		ctx:      context.Background(),
		stop:     make(chan struct{}),
		contract: contract,
		cfg:      cfg,
		headerSelector: NewConfirmedHeaderSelector(HeaderSelectorConfig{
			ConfDepth:    cfg.ConfDepth,
			MaxBatchSize: cfg.MaxHeaderBatchSize,
		}),
		database: database,
		backend:  client,
	}, nil
}
