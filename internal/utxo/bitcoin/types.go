package bitcoin

import (
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
)

// TransactionOutputResolverFactory creates resolvers for looking up transaction outputs.
type TransactionOutputResolverFactory interface {
	New() *chain.TransactionOutputResolver
}
