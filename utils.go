package main

import (
	"context"
	"errors"
	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/jszwec/csvutil"
	"github.com/segmentio/kafka-go"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// locateStartBlock is a util for locating the start block by start timestamp
func locateStartBlock(cli *tron.TronClient, startTimestamp uint64) uint64 {
	latestBlock := cli.GetJSONBlockByNumberWithTxIDs(nil)
	top := latestBlock.Number
	half := uint64(*top) / 2
	estimateStartNumber := half
	for {
		block := cli.GetJSONBlockByNumberWithTxIDs(new(big.Int).SetUint64(estimateStartNumber))
		if block == nil {
			break
		}
		log.Println(half, block.Timestamp)
		var timestamp uint64
		timestamp = uint64(*block.Timestamp)

		if timestamp < startTimestamp && startTimestamp-timestamp < 60 {
			break
		}

		//
		if timestamp < startTimestamp {
			log.Printf("%d is too small: %d", estimateStartNumber, timestamp)
			half = half / 2
			estimateStartNumber = estimateStartNumber + half
		} else {
			log.Printf("%d is too large: %d", estimateStartNumber, timestamp)
			half = half / 2
			estimateStartNumber = estimateStartNumber - half
		}

		if half == 0 || estimateStartNumber >= uint64(*top) {
			panic("failed to find the block on that timestamp")
		}
	}

	return estimateStartNumber
}

// locateEndBlock is a util for locating the end block by end timestamp
func locateEndBlock(cli *tron.TronClient, endTimestamp uint64) uint64 {
	latestBlock := cli.GetJSONBlockByNumberWithTxIDs(nil)
	top := latestBlock.Number
	half := uint64(*top) / 2
	estimateEndNumber := half
	for {
		block := cli.GetJSONBlockByNumberWithTxIDs(new(big.Int).SetUint64(estimateEndNumber))
		if block == nil {
			break
		}
		log.Println(half, block.Timestamp)
		var timestamp uint64
		timestamp = uint64(*block.Timestamp)

		if timestamp > endTimestamp && timestamp-endTimestamp < 60 {
			break
		}

		//
		if timestamp < endTimestamp {
			log.Printf("%d is too small: %d", estimateEndNumber, timestamp)
			half = half / 2
			estimateEndNumber = estimateEndNumber + half
		} else {
			log.Printf("%d is too large: %d", estimateEndNumber, timestamp)
			half = half / 2
			estimateEndNumber = estimateEndNumber - half
		}

		if half == 0 || estimateEndNumber >= uint64(*top) {
			panic("failed to find the block on that timestamp")
		}
	}

	return estimateEndNumber
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
