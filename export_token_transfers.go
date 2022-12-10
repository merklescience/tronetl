package main

import (
	"encoding/csv"
	"io"
	"log"
	"math/big"

	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/jszwec/csvutil"
	"golang.org/x/exp/slices"
)

type ExportTransferOptions struct {
	// outputType string // failed to tfOutput to a conn with
	tfOutput         io.Writer
	logOutput        io.Writer
	internalTxOutput io.Writer
	// withBlockOutput io.Writer

	ProviderURI string `json:"provider_uri,omitempty"`
	StartBlock  uint64 `json:"start_block,omitempty"`
	EndBlock    uint64 `json:"end_block,omitempty"`

	// extension
	StartTimestamp uint64 `json:"start_timestamp,omitempty"`
	EndTimestamp   uint64 `json:"end_timestamp,omitempty"`

	Contracts []string `json:"contracts,omitempty"`
}

func exportTransfers(options *ExportTransferOptions) {
	cli := tron.NewTronClient(options.ProviderURI)

	tfWriter := csv.NewWriter(options.tfOutput)
	defer tfWriter.Flush()
	tfEncoder := csvutil.NewEncoder(tfWriter)

	logWriter := csv.NewWriter(options.tfOutput)
	defer logWriter.Flush()
	logEncoder := csvutil.NewEncoder(logWriter)

	internalTxWriter := csv.NewWriter(options.tfOutput)
	defer internalTxWriter.Flush()
	internalTxEncoder := csvutil.NewEncoder(internalTxWriter)

	if options.StartTimestamp != 0 {
		// fast locate estimate start height

		estimateStartNumber := locateStartBlock(cli, options.StartTimestamp)

		for number := estimateStartNumber; ; number++ {
			block := cli.GetJSONBlockByNumberWithTxIDs(new(big.Int).SetUint64(number))
			if block == nil {
				break
			}

			blockTime := uint64(*block.Timestamp) / 1000

			if blockTime < options.StartTimestamp {
				log.Printf("passed start block %d: %d", number, *block.Timestamp)
				continue
			}

			options.StartBlock = number
			break
		}
	}

	if options.EndTimestamp != 0 {
		estimateEndNumber := locateEndBlock(cli, options.EndTimestamp)

		for number := estimateEndNumber; ; number-- {
			block := cli.GetJSONBlockByNumberWithTxIDs(new(big.Int).SetUint64(number))
			if block == nil {
				break
			}

			blockTime := uint64(*block.Timestamp) / 1000

			if blockTime > options.EndBlock {
				log.Printf("passed end block %d: %d", number, *block.Timestamp)
				continue
			}

			options.StartBlock = number
			break
		}
	}

	log.Printf("try parsing token transfers from block %d to %d", options.StartBlock, options.EndBlock)

	if options.StartBlock != 0 && options.EndBlock != 0 {
		for number := options.StartBlock; number <= options.EndBlock; number++ {
			txInfos := cli.GetTxInfosByNumber(number)
			for _, txInfo := range txInfos {
				txHash := txInfo.ID
				for logIndex, log := range txInfo.Log {
					if len(options.Contracts) != 0 && !slices.Contains(options.Contracts, hex2TAddr(log.Address)) {
						continue
					}

					tf := ExtractTransferFromLog(log.Topics, log.Data, log.Address, uint(logIndex), txHash, number)
					if tf != nil {
						err := tfEncoder.Encode(tf)
						chk(err)
					}

					err := logEncoder.Encode(NewCsvLog(number, txHash, uint(logIndex), log))
					chk(err)
				}

				for internalIndex, internalTx := range txInfo.InternalTransactions {
					err := internalTxEncoder.Encode(NewCsvInternalTx(uint(internalIndex), internalTx))
					chk(err)
				}
			}

			log.Printf("parsed block %d", number)
		}

		return
	}

}
