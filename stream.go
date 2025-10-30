package main

import (
	"encoding/json"
	"log"
	"math/big"
	"time"

	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"golang.org/x/exp/slices"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type ExportStreamOptions struct {
	ProviderURI                   string   `json:"provider_uri,omitempty"`
	StartBlock                    uint64   `json:"start_block,omitempty"`
	EndBlock                      uint64   `json:"end_block,omitempty"`
	LastSyncedBlockFile           string   `json:"last_synced_block,omitempty"`
	Contracts                     []string `json:"contracts,omitempty"`
	BlocksTopicName               string   `json:"blocks_topic_name"`
	TransactionsTopicName         string   `json:"transactions_topic_name"`
	InternalTransactionsTopicName string   `json:"internal_transactions_topic_name"`
	Trc10TopicName                string   `json:"trc10_topic_name"`
	TokenTransfersTopicName       string   `json:"token_transfers_topic_name"`
	Lag                           uint8    `json:"lag"`
}

func ExportStream(options *ExportStreamOptions) {
	cli := tron.NewTronClient(options.ProviderURI)
	// var tfEncoder, logEncoder, internalTxEncoder, receiptEncoder *csvutil.Encoder
	filterLogContracts := make([]string, len(options.Contracts))
	for i, addr := range options.Contracts {
		filterLogContracts[i] = tron.EnsureHexAddr(addr)[2:]
	}

	// Get initial latest block
	latestBlockNum, err := cli.GetLatestBlock()
	if err != nil {
		log.Printf("Error getting initial latest block: %v", err)
		return
	}
	latestBlock := latestBlockNum - uint64(options.Lag)

	startBlock := uint64(readLastSyncedBlock(options.LastSyncedBlockFile))
	log.Printf("try parsing blocks from block number %d", startBlock+1)
	log.Printf("Lag from tip of chain : %d", options.Lag)
	log.Printf("Latest Block : %d", latestBlock+uint64(options.Lag))

	for number := startBlock + 1; ; number++ {
		kafkaProducerConfig := constructKafkaProducer()
		num := new(big.Int).SetUint64(number)
		for latestBlock < number {
			// fmt.Printf("Waiting for new block. Current block number => %d, streaming lag => %d \n", latestBlock+uint64(options.Lag), uint64(options.Lag))
			// fmt.Println("Input starting block number => ", number)
			latestBlockNum, err := cli.GetLatestBlock()
			if err != nil {
				log.Printf("Error getting latest block: %v, retrying in 2 seconds...", err)
				time.Sleep(2 * time.Second)
				continue
			}
			latestBlock = latestBlockNum - uint64(options.Lag)
			time.Sleep(2 * time.Second)
		}

		// Get blocks with retry logic (do not skip the block)
		var jsonblock *tron.JSONBlockWithTxs
		var httpblock *tron.HTTPBlock
		for attempt := 0; ; attempt++ {
			if attempt > 0 {
				log.Printf("Retry attempt %d for block %d", attempt, number)
				time.Sleep(time.Duration(min(attempt, 5)) * 2 * time.Second)
			}

			jb, jerr := cli.GetJSONBlockByNumberWithTxs(num)
			if jerr != nil {
				log.Printf("Error getting JSON block %d: %v", number, jerr)
			}

			hb, herr := cli.GetHTTPBlockByNumber(num)
			if herr != nil {
				log.Printf("Error getting HTTP block %d: %v", number, herr)
			}

			if jb != nil && hb != nil {
				jsonblock = jb
				httpblock = hb
				break
			}
		}
		blockTime := uint64(httpblock.BlockHeader.RawData.Timestamp)
		csvBlock := NewCsvBlock(jsonblock, httpblock)
		blockHash := csvBlock.Hash
		csvTxMap := make(map[string]CsvTransaction)
		// guard against mismatch in transaction lists
		jsonTxCount := len(jsonblock.Transactions)
		httpTxCount := len(httpblock.Transactions)
		limitTx := jsonTxCount
		if httpTxCount < limitTx {
			limitTx = httpTxCount
		}
		for txIndex := 0; txIndex < limitTx; txIndex++ {
			jsontx := jsonblock.Transactions[txIndex]
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
					if err != nil {
						log.Printf("Error unmarshaling transfer params: %v, skipping...", err)
						continue
					}
					csvTf := NewCsvTRC10Transfer(blockHash, number, txIndex, callIndex, &httpblock.Transactions[txIndex], &tfParams, blockTimestamp)
					jsonTrc10Data, err := json.Marshal(csvTf)
					if err != nil {
						log.Printf("Error marshaling TRC10 transfer: %v, skipping...", err)
						continue
					}
					kafkaProducer(options.Trc10TopicName, csvTf.AssetName, string(jsonTrc10Data), kafkaProducerConfig)
				}
			}
		}

		jsonBlockData, err := json.Marshal(csvBlock)
		blkTimestamp := csvBlock.Timestamp
		if err != nil {
			log.Printf("Error marshaling block: %v, skipping block %d", err, number)
			continue
		}
		kafkaProducer(options.BlocksTopicName, "0x0000", string(jsonBlockData), kafkaProducerConfig)

		// token_transfer (retry until success)
		var txInfos []tron.HTTPTxInfo
		for attempt := 0; ; attempt++ {
			if attempt > 0 {
				log.Printf("Retry txinfos attempt %d for block %d", attempt, number)
				time.Sleep(time.Duration(min(attempt, 5)) * 2 * time.Second)
			}
			infos, ierr := cli.GetTxInfosByNumber(number)
			if ierr != nil {
				log.Printf("Error getting transaction infos for block %d: %v", number, ierr)
				continue
			}
			txInfos = infos
			break
		}
		for txIndex, txInfo := range txInfos {
			txHash := txInfo.ID
			txCSV := csvTxMap[txHash]
			// jsonTxn, err := json.Marshal(txCSV)
			resultStreamTxnReceipt := NewStreamCsvTransactionReceipt(number, txHash, uint(txIndex), txInfo.ContractAddress, txInfo.Fee, txInfo.Receipt, &txCSV)
			jsonTxnReceipt, err := json.Marshal(resultStreamTxnReceipt)
			if err != nil {
				log.Printf("Error marshaling transaction receipt: %v, skipping...", err)
				continue
			}
			kafkaProducer(options.TransactionsTopicName, "0x0000", string(jsonTxnReceipt), kafkaProducerConfig)
			for logIndex, logItem := range txInfo.Log {
				if len(filterLogContracts) != 0 && !slices.Contains(filterLogContracts, logItem.Address) {
					continue
				}
				tf := ExtractTransferFromLog(logItem.Topics, logItem.Data, logItem.Address, uint(logIndex), txHash, number, blkTimestamp)
				if tf != nil {
					jsonTransfer, err := json.Marshal(tf)
					if err != nil {
						log.Printf("Error marshaling transfer: %v, skipping...", err)
						continue
					}
					kafkaProducer(options.TokenTransfersTopicName, tf.TokenAddress, string(jsonTransfer), kafkaProducerConfig)
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
					if err != nil {
						log.Printf("Error marshaling internal transaction: %v, skipping...", err)
						continue
					}
					kafkaProducer(options.InternalTransactionsTopicName, "0x0000", string(jsonInternalTx), kafkaProducerConfig)
				}
			}
		}
		// flush to kafka first, then mark block as synced
		for kafkaProducerConfig.Flush(10000) > 0 {
			log.Printf("Still waiting to flush outstanding messages\n")
		}
		writeLastSyncedBlock(options.LastSyncedBlockFile, number)
		log.Printf("parsed block %d", number)
		kafkaProducerConfig.Close()
	}
}
