CREATE TABLE IF NOT EXISTS utxo_transactions (
    coin LowCardinality(String) CODEC(ZSTD(1)),
    network LowCardinality(String) CODEC(ZSTD(1)),
    txid FixedString(64) CODEC(ZSTD(1)),
    block_height UInt64 CODEC(ZSTD(1)),
    timestamp DateTime('UTC') CODEC(Delta(4), LZ4),
    size UInt32 CODEC(ZSTD(1)),
    vsize UInt32 CODEC(ZSTD(1)),
    version UInt32 CODEC(ZSTD(1)),
    locktime UInt32 CODEC(ZSTD(1)),
    fee UInt64 CODEC(ZSTD(1)),
    input_count UInt32 CODEC(ZSTD(1)),
    output_count UInt32 CODEC(ZSTD(1)),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(4), LZ4),

    INDEX idx_txid txid TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (coin, network)
PRIMARY KEY (coin, network, block_height, txid)
ORDER BY (coin, network, block_height, txid);

