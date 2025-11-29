# syntax=docker/dockerfile:1

FROM golang:1.25 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/utxo-history-ingester ./cmd/utxo/history-ingester
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/utxo-backfill-ingester ./cmd/utxo/backfill-ingester

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/utxo-history-ingester /usr/bin/utxo-history-ingester
COPY --from=builder /out/utxo-backfill-ingester /usr/bin/utxo-backfill-ingester

ENTRYPOINT ["/usr/bin/utxo-history-ingester"]
