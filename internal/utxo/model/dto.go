package model

type InsertBlock struct {
	Block   Block
	Txs     []Transaction
	Outputs []TransactionOutput
	Inputs  []TransactionInput
}
