// Package model defines domain models for UTXO ingestion.
package model

import "time"

// BlockStatus describes processing status of a block record.
type BlockStatus string

var (
	// BlockUnprocessed marks a block that has not been ingested yet.
	BlockUnprocessed BlockStatus = "unprocessed"
	// BlockProcessed marks a block that has been fully ingested.
	BlockProcessed BlockStatus = "processed"
)

// Block represents a blockchain block persisted to ClickHouse.
type Block struct {
	Coin       Coin
	Network    Network
	Height     uint64
	Hash       string
	Timestamp  time.Time
	Version    uint32
	MerkleRoot string
	Bits       uint32
	Nonce      uint32
	Difficulty float64
	Size       uint32
	TXCount    uint32
	Status     BlockStatus
}
