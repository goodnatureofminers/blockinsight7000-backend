package bitcoin

import (
	"context"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

type (
	// TransactionOutputResolver resolves transaction outputs, allowing callers to seed and fetch outputs by txid.
	TransactionOutputResolver interface {
		Resolve(ctx context.Context, txid string) ([]model.TransactionOutputLookup, error)
		ResolveBatch(ctx context.Context, txids []string) (map[string][]model.TransactionOutputLookup, error)
	}
	// RPCMetrics records metrics for RPC calls.
	RPCMetrics interface {
		Observe(operation string, err error, started time.Time)
	}
	// RPCClient is a thin wrapper around the Bitcoin RPC client.
	RPCClient interface {
		GetBlockCount() (count int64, err error)
		GetBlockHash(blockHeight int64) (hash *chainhash.Hash, err error)
		GetBlockVerboseTx(blockHash *chainhash.Hash) (res *btcjson.GetBlockVerboseTxResult, err error)
	}
	// ScriptDecoder decodes scriptPubKey outputs.
	ScriptDecoder interface {
		decodeAddresses(vout btcjson.Vout) ([]string, error)
	}
	// OutputConverter converts raw RPC outputs into domain outputs.
	OutputConverter interface {
		Convert(tx btcjson.TxRawResult, blockHeight uint64) ([]model.TransactionOutput, error)
		ConvertLookup(tx btcjson.TxRawResult) ([]model.TransactionOutputLookup, error)
	}
)
