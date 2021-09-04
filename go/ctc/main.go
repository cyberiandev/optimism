package main

import (
	"fmt"
	"math/big"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/ethereum-optimism/optimism/go/ctc/indexer"
	"github.com/ethereum/go-ethereum/common"
)

func fatal(err error) {
	if err != nil {
		fmt.Printf("unexpected failure: %v", err)
		os.Exit(1)
	}
}

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:5050", nil))
	}()

	service, err := indexer.NewIndexer(&indexer.Config{
		ChainID:            new(big.Int).SetUint64(1),
		EthereumHttpUrl:    "wss://mainnet.infura.io/ws/v3/<infura-token>",
		CtcAddress:         common.HexToAddress("0x4BF681894abEc828B212C906082B444Ceb2f6cf6"),
		ConfDepth:          20,
		MaxHeaderBatchSize: 100,
	})
	fatal(err)

	err = service.Start()
	fatal(err)

	service.Wait()

	fmt.Printf("Service done\n")
}
