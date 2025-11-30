package ingester

import "time"

const (
	defaultWorkerCount = 50

	randomHeightLimit = 10000

	transactionFlushThreshold = 1000
	outputFlushThreshold      = 1000
	inputFlushThreshold       = 1000

	sleepDuration             = 5 * time.Second
	longSleepDuration         = 1 * time.Minute
	idleSleepDuration         = 5 * time.Second
	postBatchSleepDuration    = 5 * time.Second
	blockBatcherCapacity      = 500
	blockBatcherFlushInterval = 30 * time.Second
	blockBatcherWorkerCount   = 1
)
