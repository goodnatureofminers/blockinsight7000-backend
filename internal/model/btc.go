package model

import "time"

// BTCBlock describes a bitcoin block stored in ClickHouse.
type BTCBlock struct {
	Node       string
	Network    string
	Height     uint32
	Hash       string
	Timestamp  time.Time
	Version    uint32
	MerkleRoot string
	Bits       uint32
	Nonce      uint32
	Difficulty float64
	Size       uint32
	TXCount    uint32
}

// BTCTransaction describes a bitcoin transaction stored in ClickHouse.
type BTCTransaction struct {
	Node        string
	Network     string
	TxID        string
	BlockHeight uint32
	Timestamp   time.Time
	Size        uint32
	VSize       uint32
	Version     uint32
	LockTime    uint32
	Fee         uint64
	InputCount  uint16
	OutputCount uint16
}

// BTCTransactionInput describes a single transaction input.
type BTCTransactionInput struct {
	Node         string
	Network      string
	BlockHeight  uint32
	BlockTime    time.Time
	TxID         string
	Index        uint32
	PrevTxID     string
	PrevVout     uint32
	Sequence     uint32
	IsCoinbase   bool
	Value        uint64
	ScriptSigHex string
	ScriptSigAsm string
	Witness      []string
	Addresses    []string
}

// BTCTransactionOutput describes a single transaction output.
type BTCTransactionOutput struct {
	Node        string
	Network     string
	BlockHeight uint32
	BlockTime   time.Time
	TxID        string
	Index       uint32
	Value       uint64
	ScriptType  string
	ScriptHex   string
	ScriptAsm   string
	Addresses   []string
}
