package main

import (
	"context"
	"errors"
	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/jszwec/csvutil"
	"github.com/segmentio/kafka-go"
	"log"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	FirstAfterTimestamp = 1
	LastBeforeTimestamp = -1
)

type BlockGenMetadata struct {
	blockNumber     uint64
	avgBlockGenTime uint64
	latestBlockTime int64
}

func IntToHex(i int64) string {
	return string(strconv.AppendInt([]byte{'0', 'x'}, i, 16))
}

func BlockNumberFromDateTime(c *tron.TronClient, dateTime string, blockType int) (*uint64, error) {
	var startBlock uint64
	var prevTimeDiff int64
	var prevBlockNumber int64
	layout := "2006-01-02 15:04:05"
	limitDateTime, err := time.Parse(layout, dateTime)
	//limitDateTime, err := now.Format(dateTime)
	if err != nil {
		return nil, err
	}

	blockGenTime, err := AvgBlockGenTime(c)
	if err != nil {
		return nil, err
	}

	approxBlockNumberInt := int64(blockGenTime.blockNumber) - (int64(blockGenTime.latestBlockTime)-limitDateTime.UTC().Unix())/int64(blockGenTime.avgBlockGenTime)
	// broader search first
	loopIter := 0
	fineLoopIter := 0
	log.Println("Starting Block Number ", approxBlockNumberInt)
	for {

		approxBlock := c.GetJSONBlockByNumberWithTxIDs(big.NewInt(approxBlockNumberInt))
		timeDiff := int64(*approxBlock.Timestamp) - limitDateTime.UTC().Unix() // approx block's time - our required date time

		// Very rare case handled
		if timeDiff == 0 {
			if blockType == LastBeforeTimestamp {
				approxBlockNumberInt -= 1
			}
			break
		}

		// Finer search once timeDiff is within average block Generation time
		//  math.Abs(float64(approxBlockNumberInt)-float64(prevBlockNumber)) < 2.0 this ensures the case when it's oscillating between 2 blocks but never getting into fine loop
		// because math.Abs(float64(timeDiff)) < float64(blockGenTime.avgBlockGenTime) is not true
		fineLoopCheck := math.Abs(float64(timeDiff)) < float64(blockGenTime.avgBlockGenTime) || fineLoopIter > 0 || math.Abs(float64(approxBlockNumberInt)-float64(prevBlockNumber)) < 2.0
		if fineLoopCheck {
			// fineLoopIter > 0 this ensures that once we only enter finer loop we don't have to carry on the
			// broad search based on average block generation time (blockGenTime.avgBlockGenTime)

			log.Println("Going for finer search after loop iter ", loopIter)

			if ((timeDiff < 0 && prevTimeDiff > 0) || (timeDiff > 0 && prevTimeDiff < 0)) && fineLoopIter > 0 {
				if (timeDiff < 0 && prevTimeDiff > 0) && blockType == FirstAfterTimestamp {
					approxBlockNumberInt += 1
				}
				if (timeDiff > 0 && prevTimeDiff < 0) && blockType == LastBeforeTimestamp {
					approxBlockNumberInt -= 1
				}
				log.Println("Found the block number to be ", approxBlockNumberInt)
				break
			}
			prevBlockNumber = approxBlockNumberInt
			if timeDiff < 0 {
				approxBlockNumberInt += 1
			} else {
				approxBlockNumberInt -= 1
			}

			fineLoopIter++
		} else {
			prevBlockNumber = approxBlockNumberInt
			approxBlockNumberInt = approxBlockNumberInt - timeDiff/int64(blockGenTime.avgBlockGenTime)
		}
		log.Println("After Callibration loop iter : ", loopIter, ", block number : ", approxBlockNumberInt, ", time difference : ", timeDiff, ", previous time diff ", prevTimeDiff)
		loopIter++
		prevTimeDiff = timeDiff

	}
	startBlock = uint64(approxBlockNumberInt)
	return &startBlock, nil
}

func AvgBlockGenTime(c *tron.TronClient) (*BlockGenMetadata, error) {
	const blockCheckNumber = 100000
	latestBlock := c.GetJSONBlockByNumberWithTxIDs(nil)
	oldBlockNumber := *latestBlock.Number - blockCheckNumber
	oldBlock := c.GetJSONBlockByNumberWithTxIDs(big.NewInt(int64(oldBlockNumber)))
	blockGenMeta := &BlockGenMetadata{blockNumber: uint64(*latestBlock.Number), avgBlockGenTime: (uint64(*latestBlock.Timestamp) - uint64(*oldBlock.Timestamp)) / blockCheckNumber, latestBlockTime: int64(*latestBlock.Timestamp)}
	return blockGenMeta, nil
}

func createCSVEncodeCh(wg *sync.WaitGroup, enc *csvutil.Encoder, maxWorker uint) chan any {
	wg.Add(1)
	ch := make(chan any, maxWorker)
	writeFn := func() {
		for {
			obj, ok := <-ch
			if !ok {
				wg.Done()
				return
			}
			err := enc.Encode(obj)
			chk(err)
		}
	}

	go writeFn()
	return ch
}

func kafkaWriter(address string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "topic-A",
		Balancer: &kafka.LeastBytes{},
	}
}

func kafkaProducer(writer *kafka.Writer) {
	const retries = 3
	for i := 0; i < retries; i++ {
		_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		})
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Fatalf("unexpected error %v", err)
		}
		break
	}

	if err := writer.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func readLastSyncedBlock(file string) int {
	if !doesFileExist(file) {
		log.Fatalf("last_synced_block.txt file is unavailable. Please create and rerun")
		os.Exit(0)
	}
	content, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}
	lastSyncedBlock, err := strconv.Atoi(strings.TrimSpace(string(content)))
	if err != nil {
		log.Fatal(err)
	}
	return lastSyncedBlock
}

func doesFileExist(fileName string) bool {
	_, error := os.Stat(fileName)

	if os.IsNotExist(error) {
		return false
	} else {
		return true
	}
}

func writeLastSyncedBlock(file string, lastSyncedBlock uint64) {
	writeToFile(file, strconv.FormatUint(lastSyncedBlock, 10)+"\n")
}

func writeToFile(file, content string) {
	err := os.WriteFile(file, []byte(content), 0644)
	if err != nil {
		log.Fatal(err)
	}
}
