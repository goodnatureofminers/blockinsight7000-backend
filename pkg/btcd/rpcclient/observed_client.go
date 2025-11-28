package rpcclient

import (
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
)

type (
	RPCMetrics interface {
		Observe(operation string, err error, started time.Time)
	}
)

type ObservedClient struct {
	client     *rpcclient.Client
	rpcMetrics RPCMetrics
}

func NewObservedClient(client *rpcclient.Client, rpcMetrics RPCMetrics) *ObservedClient {
	return &ObservedClient{
		client:     client,
		rpcMetrics: rpcMetrics,
	}
}

func (r *ObservedClient) GetBlockCount() (count int64, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_count", err, started)
	}()
	return r.client.GetBlockCount()
}

func (r *ObservedClient) GetBlockHash(blockHeight int64) (hash *chainhash.Hash, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_hash", err, started)
	}()
	return r.client.GetBlockHash(blockHeight)
}

func (r *ObservedClient) GetBlockVerboseTx(blockHash *chainhash.Hash) (res *btcjson.GetBlockVerboseTxResult, err error) {
	started := time.Now()
	defer func() {
		r.rpcMetrics.Observe("get_block_verbose_tx", err, started)
	}()
	return r.client.GetBlockVerboseTx(blockHash)
}
