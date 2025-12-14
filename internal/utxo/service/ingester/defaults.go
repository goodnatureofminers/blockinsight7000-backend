package ingester

import "time"

const (
	defaultWorkerCount = 20

	randomHeightLimit = 5000

	transactionFlushThreshold = 1000
	outputFlushThreshold      = 10_000
	inputFlushThreshold       = 10_000

	sleepDuration                = 5 * time.Second
	longSleepDuration            = 1 * time.Minute
	idleSleepDuration            = 5 * time.Second
	postBatchSleepDuration       = 5 * time.Second
	blockBatcherCapacity         = 1000
	newBlockBatcherCapacity      = 10_000
	blockBatcherFlushInterval    = 30 * time.Second
	blockBatcherWorkerCount      = 20
	followerBatcherFlushInterval = 1 * time.Second

	historyChunkSize uint64 = 50_000
)
