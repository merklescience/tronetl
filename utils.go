package main

import (
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jszwec/csvutil"
	// "github.com/segmentio/kafka-go"
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
	var stepSize int64
	limitDateTime, err := time.Parse(time.DateTime, dateTime)
	if err != nil {
		return nil, err
	}

	blockGenTime, err := AvgBlockGenTime(c)
	if err != nil {
		return nil, err
	}

	// Calculate approximate block based on block generattion time
	approxBlockNumber := int64(blockGenTime.blockNumber) - ((int64(blockGenTime.latestBlockTime) - limitDateTime.UTC().Unix()) / int64(blockGenTime.avgBlockGenTime))
	// Broader search first
	loopIter := 0
	fineLoopIter := 0
	log.Println("Starting Block Number ", approxBlockNumber, " : ", blockType, " and limitDatetime : ", dateTime)
	for {

		approxBlock := c.GetJSONBlockByNumberWithTxIDs(big.NewInt(approxBlockNumber))
		timeDiff := int64(*approxBlock.Timestamp) - limitDateTime.UTC().Unix() // approx block's time - our required date time

		// Edge case handled
		if timeDiff == 0 {
			if blockType == LastBeforeTimestamp {
				approxBlockNumber -= 1
			}
			log.Println("Found edge case of timeDiff 0 and exiting with ", approxBlockNumber)
			break
		}
		// Adapt blockgentime after every 5 iterations as most of the case, algo would find the block in 3-4 iterations
		if ((loopIter % 5) == 0) && (loopIter > 4) {
			blockGenTime.avgBlockGenTime = (uint64(math.Abs(float64(prevTimeDiff))) + uint64(math.Abs(float64(timeDiff)))) / (uint64(math.Abs(float64(prevBlockNumber - approxBlockNumber))))
		}
		// Finer search once timeDiff is within average block Generation time
		//  math.Abs(float64(approxBlockNumberInt)-float64(prevBlockNumber)) < 2.0 this ensures the case when it's oscillating between 2 blocks but never getting into fine loop
		// because math.Abs(float64(timeDiff)) < float64(blockGenTime.avgBlockGenTime) is not true
		fineLoopCheck := math.Abs(float64(timeDiff)) < float64(blockGenTime.avgBlockGenTime) || fineLoopIter > 0 || math.Abs(float64(approxBlockNumber)-float64(prevBlockNumber)) < 2.0
		if fineLoopCheck {
			// fineLoopIter > 0 this ensures that once we only enter finer loop we don't have to carry on the
			// broad search based on average block generation time (blockGenTime.avgBlockGenTime)
			log.Println("Going for finer search after loop iter ", loopIter)
			if ((timeDiff < 0 && prevTimeDiff > 0) || (timeDiff > 0 && prevTimeDiff < 0)) && fineLoopIter > 0 {
				if (timeDiff < 0 && prevTimeDiff > 0) && blockType == FirstAfterTimestamp {
					approxBlockNumber += 1
				}
				if (timeDiff > 0 && prevTimeDiff < 0) && blockType == LastBeforeTimestamp {
					approxBlockNumber -= 1
				}
				log.Println("Found the block number to be ", approxBlockNumber)
				break
			}
			prevBlockNumber = approxBlockNumber
			approxBlockNumber = approxBlockNumber - timeDiff/int64(math.Abs(float64(timeDiff)))
			fineLoopIter++
		} else {
			prevBlockNumber = approxBlockNumber
			if math.Abs(float64(timeDiff)) < 30 {
				// decrease step size to 1 if timediff is close
				stepSize = timeDiff / int64(math.Abs(float64(timeDiff)))
			} else {
				// default follow approximate blockgentime search
				stepSize = timeDiff / int64(blockGenTime.avgBlockGenTime)
			}
			// when stepSize is negative i.e we are below our target block the '-' will make it '+' and we will try higher block and vice versa in positive case
			approxBlockNumber = approxBlockNumber - stepSize // when stepSize is negative i.e we are below our target block
		}
		log.Println("After callibration loop iter : ", loopIter, ", block number : ", approxBlockNumber, ", time difference : ", timeDiff, ", previous time diff ", prevTimeDiff)
		loopIter++
		prevTimeDiff = timeDiff
	}
	startBlock = uint64(approxBlockNumber)
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

func constructKafkaProducer() *kafka.Producer {
	writer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "pkc-3w22w.us-central1.gcp.confluent.cloud:9092",
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     "xxxxx",
		"sasl.password":     "xxxxx",
		"client.id":         "tronetl",
		"go.batch.producer": true})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	return writer
}

func kafkaProducer(topic string, key string, value string, kafkaProducerConfig *kafka.Producer) {
	delivery_chan := make(chan kafka.Event, 10000)
	kafkaProducerConfig.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value)},
		delivery_chan,
	)

	go func() {
		for e := range kafkaProducerConfig.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()
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
