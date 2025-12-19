package model

import "time"

// Transaction represents a blockchain transaction with aggregated metadata.
type Transaction struct {
	Coin        Coin
	Network     Network
	TxID        string
	BlockHeight uint64
	Timestamp   time.Time
	Size        uint32
	VSize       uint32
	Version     uint32
	LockTime    uint32
	InputCount  uint32
	OutputCount uint32
}

// TransactionInput describes a reference to a previous transaction output.
type TransactionInput struct {
	Coin         Coin
	Network      Network
	BlockHeight  uint64
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

// TransactionOutput represents an output produced by a transaction.
type TransactionOutput struct {
	Coin        Coin
	Network     Network
	BlockHeight uint64
	TxID        string
	Index       uint32
	Value       uint64
	ScriptType  string
	ScriptHex   string
	ScriptAsm   string
	Addresses   []string
}

// TransactionOutputLookup represents an output produced by a transaction.
type TransactionOutputLookup struct {
	Coin      Coin
	Network   Network
	TxID      string
	Index     uint32
	Value     uint64
	Addresses []string
}
