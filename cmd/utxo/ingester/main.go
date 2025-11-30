// Package main contains placeholder entrypoint for the UTXO ingester.
package main

var config struct {
	ClickhouseDSN string `long:"clickhouse-dsn" env:"BTC_INGESTER_CLICKHOUSE_DSN" description:"clickhouse dsn"`
	Network       string `long:"network" env:"BTC_INGESTER_NETWORK" description:"network"`
	ZMQAddr       string `long:"zmq-addr" env:"BTC_INGESTER_ZMQ_ADDR" description:"zmq"`
}

func main() {
	_ = config
	// todo
}
