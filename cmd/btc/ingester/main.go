package main

var config struct {
	ClickhouseDSN string `long:"clickhouse-dsn" env:"BTC_INGESTER_CLICKHOUSE_DSN" description:"clickhouse dsn"`
	NodeName      string `long:"node-name" env:"BTC_INGESTER_NODE_NAME" description:"node name"`
	Network       string `long:"network" env:"BTC_INGESTER_NETWORK" description:"node name"`
	ZMQAddr       string `long:"zmq-addr" env:"BTC_INGESTER_ZMQ_ADDR" description:"zmq"`
}

func main() {
	// todo
}
