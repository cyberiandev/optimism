package indexer

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type Config struct {
	ChainID            *big.Int
	EthereumHttpUrl    string
	CtcAddress         common.Address
	ConfDepth          uint64
	MaxHeaderBatchSize uint64
}
