package main

import (
	"encoding/csv"
	"encoding/json"
	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/jszwec/csvutil"
	"io"
	"log"
	"math/big"
	"sync"
)

// ExportBlocksAndTransactionsOptions is the option for ExportBlocksAndTransactions func
type ExportBlocksAndTransactionsOptions struct {
	blksOutput  io.Writer
	txsOutput   io.Writer
	trc10Output io.Writer
	ProviderURI string `json:"provider_uri,omitempty"`
	StartBlock  uint64 `json:"start_block,omitempty"`
	EndBlock    uint64 `json:"end_block,omitempty"`

	// extension
	StartTimestamp string `json:"start_timestamp,omitempty"`
	EndTimestamp   string `json:"end_timestamp,omitempty"`
}

// ExportBlocksAndTransactions is the main func for handling export_blocks_and_transactions command
func ExportBlocksAndTransactions(options *ExportBlocksAndTransactionsOptions) {
	cli := tron.NewTronClient(options.ProviderURI)
	if options.StartBlock == 0 {
		number, err := BlockNumberFromDateTime(cli, options.StartTimestamp, FirstAfterTimestamp)
		if err != nil {
			panic(err)
		}
		options.StartBlock = *number
	}
	if options.EndBlock == 0 {
		number, err := BlockNumberFromDateTime(cli, options.EndTimestamp, LastBeforeTimestamp)
		if err != nil {
			panic(err)
		}
		options.EndBlock = *number
	}
	var blksCsvEncoder, txsCsvEncoder, trc10CsvEncoder *csvutil.Encoder
	if options.blksOutput != nil {
		blksCsvWriter := csv.NewWriter(options.blksOutput)
		defer blksCsvWriter.Flush()
		blksCsvEncoder = csvutil.NewEncoder(blksCsvWriter)
	}

	if options.txsOutput != nil {
		txsCsvWriter := csv.NewWriter(options.txsOutput)
		defer txsCsvWriter.Flush()
		txsCsvEncoder = csvutil.NewEncoder(txsCsvWriter)
	}

	if options.trc10Output != nil {
		trc10CsvWriter := csv.NewWriter(options.trc10Output)
		defer trc10CsvWriter.Flush()
		trc10CsvEncoder = csvutil.NewEncoder(trc10CsvWriter)
	}

	log.Printf("try parsing blocks and transactions from block %d to %d", options.StartBlock, options.EndBlock)

	for number := options.StartBlock; number <= options.EndBlock; number++ {
		num := new(big.Int).SetUint64(number)

		jsonblock := cli.GetJSONBlockByNumberWithTxs(num)
		httpblock := cli.GetHTTPBlockByNumber(num)
		blockTime := uint64(httpblock.BlockHeader.RawData.Timestamp)
		csvBlock := NewCsvBlock(jsonblock, httpblock)
		blockHash := csvBlock.Hash
		if options.txsOutput != nil || options.trc10Output != nil {
			for txIndex, jsontx := range jsonblock.Transactions {
				httptx := httpblock.Transactions[txIndex]
				if options.txsOutput != nil {
					csvTx := NewCsvTransaction(blockTime, txIndex, &jsontx, &httptx)
					err := txsCsvEncoder.Encode(csvTx)
					chk(err)
				}

				if options.trc10Output != nil {
					for callIndex, contractCall := range httptx.RawData.Contract {
						if contractCall.ContractType == "TransferAssetContract" ||
							contractCall.ContractType == "TransferContract" {
							var tfParams tron.TRC10TransferParams

							err := json.Unmarshal(contractCall.Parameter.Value, &tfParams)
							chk(err)
							csvTf := NewCsvTRC10Transfer(blockHash, number, txIndex, callIndex, &httpblock.Transactions[txIndex], &tfParams)
							err = trc10CsvEncoder.Encode(csvTf)
							chk(err)
						}
					}

				}

			}
		}

		err := blksCsvEncoder.Encode(csvBlock)
		chk(err)

		log.Printf("parsed block %d", number)
	}
}

// ExportBlocksAndTransactions is the main func for handling export_blocks_and_transactions command
func ExportBlocksAndTransactionsWithWorkers(options *ExportBlocksAndTransactionsOptions, workers uint) {
	cli := tron.NewTronClient(options.ProviderURI)

	var receiverWG sync.WaitGroup

	var blksCsvEncCh, txsCsvEncCh, trc10CsvEncCh chan any
	if options.blksOutput != nil {
		blksCsvWriter := csv.NewWriter(options.blksOutput)
		defer blksCsvWriter.Flush()
		blksCsvEncoder := csvutil.NewEncoder(blksCsvWriter)
		blksCsvEncCh = createCSVEncodeCh(&receiverWG, blksCsvEncoder, workers)
	}

	if options.txsOutput != nil {
		txsCsvWriter := csv.NewWriter(options.txsOutput)
		defer txsCsvWriter.Flush()
		txsCsvEncoder := csvutil.NewEncoder(txsCsvWriter)
		txsCsvEncCh = createCSVEncodeCh(&receiverWG, txsCsvEncoder, workers)
	}

	if options.trc10Output != nil {
		trc10CsvWriter := csv.NewWriter(options.trc10Output)
		defer trc10CsvWriter.Flush()
		trc10CsvEncoder := csvutil.NewEncoder(trc10CsvWriter)
		trc10CsvEncCh = createCSVEncodeCh(&receiverWG, trc10CsvEncoder, workers)
	}

	log.Printf("try parsing blocks and transactions from block %d to %d", options.StartBlock, options.EndBlock)

	exportWork := func(wg *sync.WaitGroup, workerID uint) {
		for number := options.StartBlock + uint64(workerID); number <= options.EndBlock; number += uint64(workers) {

			num := new(big.Int).SetUint64(number)

			jsonblock := cli.GetJSONBlockByNumberWithTxs(num)
			httpblock := cli.GetHTTPBlockByNumber(num)
			blockTime := uint64(httpblock.BlockHeader.RawData.Timestamp)
			csvBlock := NewCsvBlock(jsonblock, httpblock)
			blockHash := csvBlock.Hash
			if options.txsOutput != nil || options.trc10Output != nil {
				for txIndex, jsontx := range jsonblock.Transactions {
					httptx := httpblock.Transactions[txIndex]
					if options.txsOutput != nil {
						csvTx := NewCsvTransaction(blockTime, txIndex, &jsontx, &httptx)
						txsCsvEncCh <- csvTx
					}

					if options.trc10Output != nil {
						for callIndex, contractCall := range httptx.RawData.Contract {
							if contractCall.ContractType == "TransferAssetContract" ||
								contractCall.ContractType == "TransferContract" {
								var tfParams tron.TRC10TransferParams

								err := json.Unmarshal(contractCall.Parameter.Value, &tfParams)
								chk(err)
								csvTf := NewCsvTRC10Transfer(blockHash, number, txIndex, callIndex, &httpblock.Transactions[txIndex], &tfParams)
								trc10CsvEncCh <- csvTf
							}
						}

					}

				}
			}

			blksCsvEncCh <- csvBlock

			log.Printf("parsed block %d", number)
		}
		wg.Done()
	}

	var senderWG sync.WaitGroup
	for workerID := uint(0); workerID < workers; workerID++ {
		senderWG.Add(1)
		go exportWork(&senderWG, workerID)
	}

	senderWG.Wait()
	if options.blksOutput != nil {
		close(blksCsvEncCh)
	}
	if options.txsOutput != nil {
		close(txsCsvEncCh)
	}
	if trc10CsvEncCh != nil {
		close(trc10CsvEncCh)
	}
	receiverWG.Wait()
}

//func ExportBlocksAndTransactionsStream(options *ExportBlocksAndTransactionsStreamOptions) {
//	cli := tron.NewTronClient(options.ProviderURI)
//	var tfEncoder, logEncoder, internalTxEncoder, receiptEncoder *csvutil.Encoder
//	filterLogContracts := make([]string, len(options.Contracts))
//	for i, addr := range options.Contracts {
//		filterLogContracts[i] = tron.EnsureHexAddr(addr)[2:] // hex addr with 41 prefix
//	}
//	latestBlock := cli.GetLatestBlock()
//	startBlock := uint64(readLastSyncedBlock(options.LastSyncedBlockFile))
//	log.Printf("try parsing blocks and transactions from block %d", startBlock)
//
//	for number := startBlock + 1; ; number++ {
//		num := new(big.Int).SetUint64(number)
//		for latestBlock < number {
//			fmt.Println("Waiting for new block. Current block number => ", latestBlock)
//			fmt.Println("Input starting block number => ", number)
//			latestBlock = cli.GetLatestBlock()
//			time.Sleep(5 * time.Second)
//		}
//		jsonblock := cli.GetJSONBlockByNumberWithTxs(num)
//		httpblock := cli.GetHTTPBlockByNumber(num)
//		blockTime := uint64(httpblock.BlockHeader.RawData.Timestamp)
//		csvBlock := NewCsvBlock(jsonblock, httpblock)
//		blockHash := csvBlock.Hash
//		for txIndex, jsontx := range jsonblock.Transactions {
//			httptx := httpblock.Transactions[txIndex]
//			csvTx := NewCsvTransaction(blockTime, txIndex, &jsontx, &httptx)
//			jsonTxData, err := json.Marshal(csvTx)
//			kafkaProducer("tron-transaction-producer", "", string(jsonTxData))
//			//fmt.Println(string(jsonTxData))
//			chk(err)
//
//			for callIndex, contractCall := range httptx.RawData.Contract {
//				if contractCall.ContractType == "TransferAssetContract" ||
//					contractCall.ContractType == "TransferContract" {
//					var tfParams tron.TRC10TransferParams
//
//					err := json.Unmarshal(contractCall.Parameter.Value, &tfParams)
//					chk(err)
//					csvTf := NewCsvTRC10Transfer(blockHash, number, txIndex, callIndex, &httpblock.Transactions[txIndex], &tfParams)
//					jsonTrc10Data, err := json.Marshal(csvTf)
//					kafkaProducer("tron-trc10-producer", "", string(jsonTrc10Data))
//					//fmt.Println(string(jsonTrc10Data))
//					chk(err)
//				}
//			}
//		}
//
//		jsonBlockData, err := json.Marshal(csvBlock)
//		if err != nil {
//			fmt.Println("Error:", err)
//			return
//		}
//		kafkaProducer("tron-blocks-producer", "", string(jsonBlockData))
//		//fmt.Println(string(jsonData))
//		chk(err)
//
//		//token_transfer
//		txInfos := cli.GetTxInfosByNumber(number)
//		for txIndex, txInfo := range txInfos {
//			txHash := txInfo.ID
//
//			if receiptEncoder != nil {
//				//receiptEncoder.Encode(NewCsvReceipt(number, txHash, uint(txIndex), txInfo.ContractAddress, txInfo.Receipt))
//				resultReceipt := NewCsvReceipt(number, txHash, uint(txIndex), txInfo.ContractAddress, txInfo.Receipt)
//				jsonReceipt, err := json.Marshal(resultReceipt)
//				chk(err)
//				kafkaProducer("", "", string(jsonReceipt))
//			}
//
//			for logIndex, log := range txInfo.Log {
//				if len(filterLogContracts) != 0 && !slices.Contains(filterLogContracts, log.Address) {
//					continue
//				}
//
//				if tfEncoder != nil {
//					tf := ExtractTransferFromLog(log.Topics, log.Data, log.Address, uint(logIndex), txHash, number)
//					if tf != nil {
//						jsonTransfer, err := json.Marshal(tf)
//						chk(err)
//						kafkaProducer("", "", string(jsonTransfer))
//						chk(err)
//					}
//				}
//
//				if logEncoder != nil {
//					tf := NewCsvLog(number, txHash, uint(logIndex), log)
//					jsonLog, err := json.Marshal(tf)
//					chk(err)
//					kafkaProducer("", "", string(jsonLog))
//					chk(err)
//				}
//
//			}
//
//			if internalTxEncoder != nil {
//				for internalIndex, internalTx := range txInfo.InternalTransactions {
//					for callInfoIndex, callInfo := range internalTx.CallValueInfo {
//						internalTx := NewCsvInternalTx(number, txHash, uint(internalIndex), internalTx, uint(callInfoIndex), callInfo.TokenID, callInfo.CallValue)
//						jsonInternalTx, err := json.Marshal(internalTx)
//						chk(err)
//						kafkaProducer("", "", string(jsonInternalTx))
//						chk(err)
//					}
//				}
//			}
//
//		}
//		writeLastSyncedBlock(options.LastSyncedBlockFile, number)
//		log.Printf("parsed block %d", number)
//	}
//}
