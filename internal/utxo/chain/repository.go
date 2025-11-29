package chain

import (
	"context"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// ClickhouseRepository describes the persistence operations the ingesters need.
type ClickhouseRepository interface {
	TransactionOutputs(ctx context.Context, coin model.Coin, network model.Network, txid string) ([]model.TransactionOutput, error)
}
