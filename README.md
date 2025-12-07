# BlockInsight7000 Backend

[![Test](https://github.com/goodnatureofminers/blockinsight7000-backend/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/goodnatureofminers/blockinsight7000-backend/actions/workflows/test.yml)
[![Lint](https://github.com/goodnatureofminers/blockinsight7000-backend/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/goodnatureofminers/blockinsight7000-backend/actions/workflows/lint.yml)
![Coverage](https://raw.githubusercontent.com/goodnatureofminers/blockinsight7000-backend/main/coverage.svg?raw=4)

Backend for a blockchain explorer: ingests chain data (currently Bitcoin) into ClickHouse and serves it via gRPC/REST APIs with Prometheus metrics.

## Prerequisites
- Go 1.21+ (module-based build).
- ClickHouse reachable via DSN.
- Bitcoin Core (or compatible) RPC endpoint for UTXO data.
- Prometheus/Grafana optional for metrics consumption.

## Development
- Run linters in Docker (no local install needed): `make lint-docker`.
- Update coverage badge locally: `make coverage` (writes `coverage.out` and `coverage.svg`).
