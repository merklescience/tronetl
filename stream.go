package main

import (
	"encoding/json"
	"fmt"
	"golang.org/x/exp/slices"
	"log"
	"math/big"
	"time"

	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/jszwec/csvutil"
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
	var tfEncoder, logEncoder, internalTxEncoder, receiptEncoder *csvutil.Encoder
	filterLogContracts := make([]string, len(options.Contracts))
	for i, addr := range options.Contracts {
		filterLogContracts[i] = tron.EnsureHexAddr(addr)[2:] // hex addr with 41 prefix
	}
	latestBlock := cli.GetLatestBlock()
	startBlock := uint64(readLastSyncedBlock(options.LastSyncedBlockFile))
	log.Printf("try parsing blocks and transactions from block %d", startBlock)

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
			kafkaProducer("tron-transaction-producer", "", string(jsonTxData))
			//fmt.Println(string(jsonTxData))
			chk(err)

			for callIndex, contractCall := range httptx.RawData.Contract {
				if contractCall.ContractType == "TransferAssetContract" ||
					contractCall.ContractType == "TransferContract" {
					var tfParams tron.TRC10TransferParams

					err := json.Unmarshal(contractCall.Parameter.Value, &tfParams)
					chk(err)
					csvTf := NewCsvTRC10Transfer(blockHash, number, txIndex, callIndex, &httpblock.Transactions[txIndex], &tfParams)
					jsonTrc10Data, err := json.Marshal(csvTf)
					kafkaProducer("tron-trc10-producer", "", string(jsonTrc10Data))
					//fmt.Println(string(jsonTrc10Data))
					chk(err)
				}
			}
		}

		jsonBlockData, err := json.Marshal(csvBlock)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		kafkaProducer("tron-blocks-producer", "", string(jsonBlockData))
		//fmt.Println(string(jsonData))
		chk(err)

		//token_transfer
		txInfos := cli.GetTxInfosByNumber(number)
		for txIndex, txInfo := range txInfos {
			txHash := txInfo.ID

			if receiptEncoder != nil {
				//receiptEncoder.Encode(NewCsvReceipt(number, txHash, uint(txIndex), txInfo.ContractAddress, txInfo.Receipt))
				resultReceipt := NewCsvReceipt(number, txHash, uint(txIndex), txInfo.ContractAddress, txInfo.Receipt)
				jsonReceipt, err := json.Marshal(resultReceipt)
				chk(err)
				kafkaProducer("", "", string(jsonReceipt))
			}

			for logIndex, log := range txInfo.Log {
				if len(filterLogContracts) != 0 && !slices.Contains(filterLogContracts, log.Address) {
					continue
				}

				if tfEncoder != nil {
					tf := ExtractTransferFromLog(log.Topics, log.Data, log.Address, uint(logIndex), txHash, number)
					if tf != nil {
						jsonTransfer, err := json.Marshal(tf)
						chk(err)
						kafkaProducer("", "", string(jsonTransfer))
						chk(err)
					}
				}

				if logEncoder != nil {
					tf := NewCsvLog(number, txHash, uint(logIndex), log)
					jsonLog, err := json.Marshal(tf)
					chk(err)
					kafkaProducer("", "", string(jsonLog))
					chk(err)
				}

			}

			if internalTxEncoder != nil {
				for internalIndex, internalTx := range txInfo.InternalTransactions {
					for callInfoIndex, callInfo := range internalTx.CallValueInfo {
						internalTx := NewCsvInternalTx(number, txHash, uint(internalIndex), internalTx, uint(callInfoIndex), callInfo.TokenID, callInfo.CallValue)
						jsonInternalTx, err := json.Marshal(internalTx)
						chk(err)
						kafkaProducer("", "", string(jsonInternalTx))
						chk(err)
					}
				}
			}

		}
		writeLastSyncedBlock(options.LastSyncedBlockFile, number)
		log.Printf("parsed block %d", number)
	}
}
