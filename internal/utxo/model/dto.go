package model

// InsertBlock groups a block with its transactions and related inputs/outputs for batch insertion.
type InsertBlock struct {
	Block   Block
	Txs     []Transaction
	Outputs []TransactionOutput
	Inputs  []TransactionInput
}
