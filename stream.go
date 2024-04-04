package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"

	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"golang.org/x/exp/slices"
)

type ExportStreamOptions struct {
	ProviderURI         string   `json:"provider_uri,omitempty"`
	StartBlock          uint64   `json:"start_block,omitempty"`
	EndBlock            uint64   `json:"end_block,omitempty"`
	LastSyncedBlockFile string   `json:"last_synced_block,omitempty"`
	Contracts           []string `json:"contracts,omitempty"`
}

func ExportStream(options *ExportStreamOptions) {
	cli := tron.NewTronClient(options.ProviderURI)
	// var tfEncoder, logEncoder, internalTxEncoder, receiptEncoder *csvutil.Encoder
	filterLogContracts := make([]string, len(options.Contracts))
	for i, addr := range options.Contracts {
		filterLogContracts[i] = tron.EnsureHexAddr(addr)[2:] // hex addr with 41 prefix
	}
	latestBlock := cli.GetLatestBlock()
	startBlock := uint64(readLastSyncedBlock(options.LastSyncedBlockFile))
	log.Printf("try parsing blocks from block number %d", startBlock+1)
	kafkaProducerConfig := constructKafkaProducer()

	for number := startBlock + 1; ; number++ {
		num := new(big.Int).SetUint64(number)
		for latestBlock < number {
			fmt.Println("Waiting for new block. Current block number => ", latestBlock)
			fmt.Println("Input starting block number => ", number)
			latestBlock = cli.GetLatestBlock()
			time.Sleep(5 * time.Second)
		}
		jsonblock := cli.GetJSONBlockByNumberWithTxs(num)
		httpblock := cli.GetHTTPBlockByNumber(num)
		blockTime := uint64(httpblock.BlockHeader.RawData.Timestamp)
		csvBlock := NewCsvBlock(jsonblock, httpblock)
		blockHash := csvBlock.Hash
		for txIndex, jsontx := range jsonblock.Transactions {
			httptx := httpblock.Transactions[txIndex]
			csvTx := NewCsvTransaction(blockTime, txIndex, &jsontx, &httptx)
			jsonTxData, err := json.Marshal(csvTx)
			kafkaProducer("producer-tron-transactions-hot-rpc", "", string(jsonTxData), kafkaProducerConfig)
			chk(err)

			for callIndex, contractCall := range httptx.RawData.Contract {
				if contractCall.ContractType == "TransferAssetContract" ||
					contractCall.ContractType == "TransferContract" {
					var tfParams tron.TRC10TransferParams

					err := json.Unmarshal(contractCall.Parameter.Value, &tfParams)
					chk(err)
					csvTf := NewCsvTRC10Transfer(blockHash, number, txIndex, callIndex, &httpblock.Transactions[txIndex], &tfParams)
					jsonTrc10Data, err := json.Marshal(csvTf)
					kafkaProducer("producer-tron-trc10-hot-rpc", "", string(jsonTrc10Data), kafkaProducerConfig)
					chk(err)
				}
			}
		}

		jsonBlockData, err := json.Marshal(csvBlock)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		kafkaProducer("producer-tron-blocks-hot-rpc", "", string(jsonBlockData), kafkaProducerConfig)
		chk(err)

		//token_transfer
		txInfos := cli.GetTxInfosByNumber(number)
		for txIndex, txInfo := range txInfos {
			txHash := txInfo.ID
			resultReceipt := NewCsvReceipt(number, txHash, uint(txIndex), txInfo.ContractAddress, txInfo.Receipt)
			jsonReceipt, err := json.Marshal(resultReceipt)
			chk(err)
			kafkaProducer("producer-tron-receipt-hot-rpc", "", string(jsonReceipt), kafkaProducerConfig)
			for logIndex, log := range txInfo.Log {
				if len(filterLogContracts) != 0 && !slices.Contains(filterLogContracts, log.Address) {
					continue
				}

				tf := ExtractTransferFromLog(log.Topics, log.Data, log.Address, uint(logIndex), txHash, number)
				if tf != nil {
					jsonTransfer, err := json.Marshal(tf)
					chk(err)
					kafkaProducer("producer-tron-token_transfers-hot-rpc", "", string(jsonTransfer), kafkaProducerConfig)
					chk(err)
				}

				tfLog := NewCsvLog(number, txHash, uint(logIndex), log)
				jsonLog, err := json.Marshal(tfLog)
				chk(err)
				kafkaProducer("producer-tron-logs-hot-rpc", "", string(jsonLog), kafkaProducerConfig)
				chk(err)
			}
			for internalIndex, internalTx := range txInfo.InternalTransactions {
				for callInfoIndex, callInfo := range internalTx.CallValueInfo {
					internalTx := NewCsvInternalTx(number, txHash, uint(internalIndex), internalTx, uint(callInfoIndex), callInfo.TokenID, callInfo.CallValue)
					jsonInternalTx, err := json.Marshal(internalTx)
					chk(err)
					kafkaProducer("producer-tron-internal_transactions-hot-rpc", "", string(jsonInternalTx), kafkaProducerConfig)
					chk(err)
				}
			}
		}
		writeLastSyncedBlock(options.LastSyncedBlockFile, number)
		log.Printf("parsed block %d", number)
	}
}
