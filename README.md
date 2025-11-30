# BlockInsight7000 Backend

![Coverage](https://raw.githubusercontent.com/goodnatureofminers/blockinsight7000-backend/main/coverage.svg)

Backend for a blockchain explorer: ingests chain data (currently Bitcoin) into ClickHouse and serves it via gRPC/REST APIs with Prometheus metrics.

## Prerequisites
- Go 1.21+ (module-based build).
- ClickHouse reachable via DSN.
- Bitcoin Core (or compatible) RPC endpoint for UTXO data.
- Prometheus/Grafana optional for metrics consumption.

## Development
- Run linters in Docker (no local install needed): `make lint-docker`.
