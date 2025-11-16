package service

import (
	"context"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/model"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

type (
	BTCRepository interface {
		TransactionOutputs(ctx context.Context, node, network, txid string) ([]model.BTCTransactionOutput, error)
		MaxBlockHeight(ctx context.Context, node, network string) (uint64, bool, error)
		InsertBlocks(ctx context.Context, blocks []model.BTCBlock) error
		InsertTransactions(ctx context.Context, txs []model.BTCTransaction) error
		InsertTransactionOutputs(ctx context.Context, outputs []model.BTCTransactionOutput) error
		InsertTransactionInputs(ctx context.Context, inputs []model.BTCTransactionInput) error
	}
	BTCRpcClient interface {
		GetBlockCount() (int64, error)
		GetBlockHash(blockHeight int64) (*chainhash.Hash, error)
		GetBlockVerboseTx(blockHash *chainhash.Hash) (*btcjson.GetBlockVerboseTxResult, error)
	}
)
