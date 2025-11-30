package model

// Coin identifies a blockchain protocol symbol (e.g. BTC).
type Coin string

// Network identifies a blockchain network (e.g. mainnet or testnet).
type Network string

var (
	// BTC represents Bitcoin.
	BTC Coin = "BTC"
	// LTC represents Litecoin.
	LTC Coin = "LTC"
	// RVN represents Ravencoin.
	RVN Coin = "RVN"
)

var (
	// Testnet identifies a test network.
	Testnet Network = "testnet"
	// Mainnet identifies the main production network.
	Mainnet Network = "mainnet"
)
