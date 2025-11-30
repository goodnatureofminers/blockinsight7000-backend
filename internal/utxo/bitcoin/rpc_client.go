package bitcoin

import (
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
)

type (
	// RPCMetrics records metrics for RPC calls.
	RPCMetrics interface {
		Observe(operation string, err error, started time.Time)
	}
)

// RPCClient wraps btc rpcclient with metrics instrumentation.
type RPCClient struct {
	client     *rpcclient.Client
	rpcMetrics RPCMetrics
}

// NewRPCClient constructs an instrumented RPC client.
func NewRPCClient(client *rpcclient.Client, rpcMetrics RPCMetrics) *RPCClient {
	return &RPCClient{
		client:     client,
		rpcMetrics: rpcMetrics,
	}
}

// GetBlockCount returns the latest block count.
func (r *RPCClient) GetBlockCount() (count int64, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_count", err, started)
	}()
	return r.client.GetBlockCount()
}

// GetBlockHash returns the block hash for a height.
func (r *RPCClient) GetBlockHash(blockHeight int64) (hash *chainhash.Hash, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_hash", err, started)
	}()
	return r.client.GetBlockHash(blockHeight)
}

// GetBlockVerboseTx returns a verbose block with transactions.
func (r *RPCClient) GetBlockVerboseTx(blockHash *chainhash.Hash) (res *btcjson.GetBlockVerboseTxResult, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_verbose_tx", err, started)
	}()
	return r.client.GetBlockVerboseTx(blockHash)
}
