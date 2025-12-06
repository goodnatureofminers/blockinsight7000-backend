package bitcoin

import (
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

// rpcClient wraps btc rpcclient with metrics instrumentation.
type rpcClient struct {
	client     RPCClient
	rpcMetrics RPCMetrics
}

// NewRPCClient constructs an instrumented RPC client.
func NewRPCClient(client RPCClient, rpcMetrics RPCMetrics) RPCClient {
	return &rpcClient{
		client:     client,
		rpcMetrics: rpcMetrics,
	}
}

// GetBlockCount returns the latest block count.
func (r *rpcClient) GetBlockCount() (count int64, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_count", err, started)
	}()
	return r.client.GetBlockCount()
}

// GetBlockHash returns the block hash for a height.
func (r *rpcClient) GetBlockHash(blockHeight int64) (hash *chainhash.Hash, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_hash", err, started)
	}()
	return r.client.GetBlockHash(blockHeight)
}

// GetBlockVerboseTx returns a verbose block with transactions.
func (r *rpcClient) GetBlockVerboseTx(blockHash *chainhash.Hash) (res *btcjson.GetBlockVerboseTxResult, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_verbose_tx", err, started)
	}()
	return r.client.GetBlockVerboseTx(blockHash)
}
