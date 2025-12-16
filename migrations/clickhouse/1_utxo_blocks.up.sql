CREATE TABLE IF NOT EXISTS utxo_blocks (
    coin LowCardinality(String) CODEC(ZSTD(1)),
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
    status LowCardinality(String) CODEC(ZSTD(1)),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(4), LZ4),

    INDEX idx_hash hash TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (coin, network)
PRIMARY KEY (coin, network, height)
ORDER BY (coin, network, height);
