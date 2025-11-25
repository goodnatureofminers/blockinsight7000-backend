# syntax=docker/dockerfile:1

FROM golang:1.25 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/btc-history-ingester ./cmd/btc/history-ingester

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/btc-history-ingester /usr/bin/btc-history-ingester

ENTRYPOINT ["/usr/bin/btc-history-ingester"]
