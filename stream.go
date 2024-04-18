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

	for number := startBlock + 1; ; number++ {
		kafkaProducerConfig := constructKafkaProducer()
		num := new(big.Int).SetUint64(number)
		for latestBlock < number {
			fmt.Println("Waiting for new block. Current block number => ", latestBlock)
			fmt.Println("Input starting block number => ", number)
			latestBlock = cli.GetLatestBlock()
			time.Sleep(5 * time.Second)
		}
		jsonblock := cli.GetJSONBlockByNumberWithTxs(num)
		httpblock := cli.GetHTTPBlockByNumber(num)
		if httpblock == nil || jsonblock == nil {
			time.Sleep(10 * time.Second)
			jsonblock = cli.GetJSONBlockByNumberWithTxs(num)
			httpblock = cli.GetHTTPBlockByNumber(num)

		}
		blockTime := uint64(httpblock.BlockHeader.RawData.Timestamp)
		csvBlock := NewCsvBlock(jsonblock, httpblock)
		blockHash := csvBlock.Hash
		csvTxMap := make(map[string]CsvTransaction)
		for txIndex, jsontx := range jsonblock.Transactions {
			httptx := httpblock.Transactions[txIndex]
			csvTx := NewCsvTransaction(blockTime, txIndex, &jsontx, &httptx)
			blockTimestamp := csvTx.BlockTimestamp
			csvTxMap[csvTx.Hash] = *csvTx
			// jsonTxData, err := json.Marshal(csvTx)

			// kafkaProducer("producer-tron-transactions-hot-rpc", "", string(jsonTxData), kafkaProducerConfig)
			// chk(err)

			for callIndex, contractCall := range httptx.RawData.Contract {
				if contractCall.ContractType == "TransferAssetContract" ||
					contractCall.ContractType == "TransferContract" {
					var tfParams tron.TRC10TransferParams

					err := json.Unmarshal(contractCall.Parameter.Value, &tfParams)
					chk(err)
					csvTf := NewCsvTRC10Transfer(blockHash, number, txIndex, callIndex, &httpblock.Transactions[txIndex], &tfParams, blockTimestamp)
					jsonTrc10Data, err := json.Marshal(csvTf)
					kafkaProducer("producer-tron_dev-trc10-hot", csvTf.AssetName, string(jsonTrc10Data), kafkaProducerConfig)
					chk(err)
				}
			}
		}

		jsonBlockData, err := json.Marshal(csvBlock)
		blkTimestamp := csvBlock.Timestamp
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		kafkaProducer("producer-tron_dev-blocks-hot", "0x0000", string(jsonBlockData), kafkaProducerConfig)
		chk(err)

		//token_transfer
		txInfos := cli.GetTxInfosByNumber(number)
		for txIndex, txInfo := range txInfos {
			txHash := txInfo.ID
			txCSV := csvTxMap[txHash]
			// jsonTxn, err := json.Marshal(txCSV)
			resultStreamTxnReceipt := NewStreamCsvTransactionReceipt(number, txHash, uint(txIndex), txInfo.ContractAddress, txInfo.Fee, txInfo.Receipt, &txCSV)
			jsonTxnReceipt, err := json.Marshal(resultStreamTxnReceipt)
			chk(err)
			kafkaProducer("producer-tron_dev-transactions-hot", "0x0000", string(jsonTxnReceipt), kafkaProducerConfig)
			chk(err)
			for logIndex, log := range txInfo.Log {
				if len(filterLogContracts) != 0 && !slices.Contains(filterLogContracts, log.Address) {
					continue
				}
				tf := ExtractTransferFromLog(log.Topics, log.Data, log.Address, uint(logIndex), txHash, number, blkTimestamp)
				if tf != nil {
					jsonTransfer, err := json.Marshal(tf)
					chk(err)
					kafkaProducer("producer-tron_dev-token_transfers-hot", tf.TokenAddress, string(jsonTransfer), kafkaProducerConfig)
					chk(err)
				}

				// tfLog := NewCsvLog(number, txHash, uint(logIndex), log)
				// jsonLog, err := json.Marshal(tfLog)
				// chk(err)
				// kafkaProducer("producer-tron-logs-hot", "", string(jsonLog), kafkaProducerConfig)
				// chk(err)
			}
			for internalIndex, internalTx := range txInfo.InternalTransactions {
				for callInfoIndex, callInfo := range internalTx.CallValueInfo {
					internalTx := NewCsvInternalTx(number, txHash, uint(internalIndex), internalTx, uint(callInfoIndex), callInfo.TokenID, callInfo.CallValue, blkTimestamp)
					jsonInternalTx, err := json.Marshal(internalTx)
					chk(err)
					kafkaProducer("producer-tron_dev-internal_transactions-hot", "0x0000", string(jsonInternalTx), kafkaProducerConfig)
					chk(err)
				}
			}
		}
		writeLastSyncedBlock(options.LastSyncedBlockFile, number)
		log.Printf("parsed block %d", number)
		for kafkaProducerConfig.Flush(10000) > 0 {
			fmt.Print("Still waiting to flush outstanding messages\n")
		}
		kafkaProducerConfig.Close()
	}
}
