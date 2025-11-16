CREATE TABLE IF NOT EXISTS btc_blocks (
    node LowCardinality(String),
    network LowCardinality(String),
    height UInt32,
    hash FixedString(64),
    timestamp DateTime,
    version UInt32,
    merkleroot FixedString(64),
    bits UInt32,
    nonce UInt32,
    difficulty Float64,
    size UInt32,
    tx_count UInt32
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (node, network, height)
ORDER BY (node, network, height);