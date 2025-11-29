package model

import "time"

type BlockStatus string

var (
	BlockUnprocessed BlockStatus = "unprocessed"
	BlockProcessed   BlockStatus = "processed"
)

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
