package repository

import (
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/metrics"
)

// BTCNodeRepository wraps rpcclient.Client to provide metrics for BTCRpcClient interface.
type BTCNodeRepository struct {
	client  *rpcclient.Client
	network string
}

func NewBTCNodeRepository(client *rpcclient.Client, network string) *BTCNodeRepository {
	return &BTCNodeRepository{
		client:  client,
		network: network,
	}
}

func (r *BTCNodeRepository) GetBlockCount() (count int64, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveBTCRPC("get_block_count", r.network, err, started)
	}()
	return r.client.GetBlockCount()
}

func (r *BTCNodeRepository) GetBlockHash(blockHeight int64) (hash *chainhash.Hash, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveBTCRPC("get_block_hash", r.network, err, started)
	}()
	return r.client.GetBlockHash(blockHeight)
}

func (r *BTCNodeRepository) GetBlockVerboseTx(blockHash *chainhash.Hash) (res *btcjson.GetBlockVerboseTxResult, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveBTCRPC("get_block_verbose_tx", r.network, err, started)
	}()
	return r.client.GetBlockVerboseTx(blockHash)
}
