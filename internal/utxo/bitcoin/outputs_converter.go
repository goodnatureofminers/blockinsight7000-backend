package bitcoin

import (
	"fmt"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/safe"
)

// outputConverter converts rpc tx outputs to domain outputs using a decoder.
type outputConverter struct {
	decoder ScriptDecoder
	network model.Network
}

// NewOutputConverter constructs a converter that turns raw RPC outputs into domain outputs for the given network.
func NewOutputConverter(decoder ScriptDecoder, network model.Network) OutputConverter {
	return &outputConverter{decoder: decoder, network: network}
}

func (c *outputConverter) Convert(tx btcjson.TxRawResult, blockHeight uint64) ([]model.TransactionOutput, error) {
	outputs := make([]model.TransactionOutput, 0, len(tx.Vout))
	for idx, vout := range tx.Vout {
		if vout.Value < 0 {
			return nil, fmt.Errorf("tx %s output %d negative value: %f", tx.Txid, idx, vout.Value)
		}

		index, err := safe.Uint32(idx)
		if err != nil {
			return nil, fmt.Errorf("tx %s output index overflow: %w", tx.Txid, err)
		}

		value, err := BtcToSatoshis(vout.Value)
		if err != nil {
			return nil, fmt.Errorf("tx %s output %d safe value: %w", tx.Txid, idx, err)
		}
		addresses, err := c.decoder.decodeAddresses(vout)
		if err != nil {
			return nil, fmt.Errorf("decode addresses for tx %s output %d: %w", tx.Txid, idx, err)
		}

		outputs = append(outputs, model.TransactionOutput{
			Coin:        model.BTC,
			Network:     c.network,
			BlockHeight: blockHeight,
			TxID:        tx.Txid,
			Index:       index,
			Value:       value,
			ScriptType:  vout.ScriptPubKey.Type,
			ScriptHex:   vout.ScriptPubKey.Hex,
			ScriptAsm:   vout.ScriptPubKey.Asm,
			Addresses:   addresses,
		})
	}
	return outputs, nil
}

func (c *outputConverter) ConvertLookup(tx btcjson.TxRawResult) ([]model.TransactionOutputLookup, error) {
	outputs := make([]model.TransactionOutputLookup, 0, len(tx.Vout))
	for idx, vout := range tx.Vout {
		if vout.Value < 0 {
			return nil, fmt.Errorf("tx %s output %d negative value: %f", tx.Txid, idx, vout.Value)
		}

		index, err := safe.Uint32(idx)
		if err != nil {
			return nil, fmt.Errorf("tx %s output index overflow: %w", tx.Txid, err)
		}

		value, err := BtcToSatoshis(vout.Value)
		if err != nil {
			return nil, fmt.Errorf("tx %s output %d safe value: %w", tx.Txid, idx, err)
		}
		addresses, err := c.decoder.decodeAddresses(vout)
		if err != nil {
			return nil, fmt.Errorf("decode addresses for tx %s output %d: %w", tx.Txid, idx, err)
		}

		outputs = append(outputs, model.TransactionOutputLookup{
			Coin:      model.BTC,
			Network:   c.network,
			TxID:      tx.Txid,
			Index:     index,
			Value:     value,
			Addresses: addresses,
		})
	}
	return outputs, nil
}
