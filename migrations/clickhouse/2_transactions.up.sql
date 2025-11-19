CREATE TABLE IF NOT EXISTS btc_transactions (
    network LowCardinality(String) CODEC(ZSTD(1)),
    txid FixedString(64) CODEC(ZSTD(1)),
    block_height UInt64 CODEC(ZSTD(1)),
    timestamp DateTime('UTC') CODEC(Delta(4), LZ4),
    size UInt32 CODEC(ZSTD(1)),
    vsize UInt32 CODEC(ZSTD(1)),
    version UInt32 CODEC(ZSTD(1)),
    locktime UInt32 CODEC(ZSTD(1)),
    input_count UInt16 CODEC(ZSTD(1)),
    output_count UInt16 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY (network, toYYYYMM(timestamp))
PRIMARY KEY (network, block_height, txid)
ORDER BY (network, block_height, txid);
