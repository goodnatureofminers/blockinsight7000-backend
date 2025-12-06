package bitcoin

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// scriptDecoder extracts human-readable addresses from ScriptPubKey results.
type scriptDecoder struct {
	params *chaincfg.Params
}

// NewScriptDecoder initializes a decoder for extracting addresses using params of the provided network.
func NewScriptDecoder(network model.Network) (ScriptDecoder, error) {
	params, err := chainParamsForNetwork(network)
	if err != nil {
		return nil, err
	}
	return &scriptDecoder{params: params}, nil
}

func (d *scriptDecoder) decodeAddresses(vout btcjson.Vout) ([]string, error) {
	if len(vout.ScriptPubKey.Addresses) > 0 {
		return append([]string(nil), vout.ScriptPubKey.Addresses...), nil
	}
	if vout.ScriptPubKey.Address != "" {
		return []string{vout.ScriptPubKey.Address}, nil
	}
	if vout.ScriptPubKey.Hex == "" {
		return nil, nil
	}

	scriptBytes, err := hex.DecodeString(vout.ScriptPubKey.Hex)
	if err != nil {
		return nil, err
	}
	_, addrs, _, err := txscript.ExtractPkScriptAddrs(scriptBytes, d.params)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		result = append(result, addr.EncodeAddress())
	}
	return result, nil
}

func chainParamsForNetwork(network model.Network) (*chaincfg.Params, error) {
	switch strings.ToLower(string(network)) {
	case "main", "mainnet", "bitcoin":
		return &chaincfg.MainNetParams, nil
	case "testnet", "testnet3":
		return &chaincfg.TestNet3Params, nil
	case "regtest":
		return &chaincfg.RegressionNetParams, nil
	case "signet":
		return &chaincfg.SigNetParams, nil
	default:
		return nil, fmt.Errorf("unsupported network %q", network)
	}
}
