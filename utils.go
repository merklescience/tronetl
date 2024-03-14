package main

import (
	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/jszwec/csvutil"
	"log"
	"math"
	"math/big"
	"strconv"
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
	checkTime       int64
}

func IntToHex(i int64) string {
	return string(strconv.AppendInt([]byte{'0', 'x'}, i, 16))
}

func BlockNumberFromDateTime(c *tron.TronClient, dateTime string, blockType int) (*uint64, error) {
	var startBlock uint64
	var prevTimeDiff int64
	var prevBlockNumber int64
	limitDateTime, err := time.Parse(time.DateTime, dateTime)

	if err != nil {
		return nil, err
	}

	blockGenTime, err := AvgBlockGenTime(c)
	if err != nil {
		return nil, err
	}

	approxBlockNumberInt := int64(blockGenTime.blockNumber) - (int64(blockGenTime.checkTime)-limitDateTime.UTC().Unix())/int64(blockGenTime.avgBlockGenTime)
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
	currentTime := uint64(time.Now().UTC().Unix())
	latestBlock := c.GetJSONBlockByNumberWithTxIDs(nil)

	oldBlockNumber := *latestBlock.Number - blockCheckNumber
	oldBlock := c.GetJSONBlockByNumberWithTxIDs(big.NewInt(int64(oldBlockNumber)))
	blockGenMeta := &BlockGenMetadata{blockNumber: uint64(*latestBlock.Number), avgBlockGenTime: (currentTime - uint64(*oldBlock.Timestamp)) / blockCheckNumber, checkTime: int64(currentTime)}
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
