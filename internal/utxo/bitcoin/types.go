package bitcoin

import (
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
)

type TransactionOutputResolverFactory interface {
	New() *chain.TransactionOutputResolver
}
