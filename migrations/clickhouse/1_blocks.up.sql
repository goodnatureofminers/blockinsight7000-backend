CREATE TABLE IF NOT EXISTS btc_blocks (
    network LowCardinality(String) CODEC(ZSTD(1)),
    height UInt64 CODEC(ZSTD(1)),
    hash FixedString(64) CODEC(ZSTD(1)),
    timestamp DateTime('UTC') CODEC(Delta(4), LZ4),
    version UInt32  CODEC(ZSTD(1)),
    merkleroot FixedString(64) CODEC(ZSTD(1)),
    bits UInt32  CODEC(ZSTD(1)),
    nonce UInt32  CODEC(ZSTD(1)),
    difficulty Float64  CODEC(ZSTD(1)),
    size UInt32  CODEC(ZSTD(1)),
    tx_count UInt32 CODEC(ZSTD(1)),
    updated_at DateTime('UTC') DEFAULT now() CODEC(Delta(4), LZ4)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (network, toYear(timestamp))
PRIMARY KEY (network, height)
ORDER BY (network, height);