CREATE TABLE IF NOT EXISTS btc_transactions (
    node LowCardinality(String),
    network LowCardinality(String),
    txid FixedString(64),
    block_height UInt32,
    timestamp DateTime,
    size UInt32,
    vsize UInt32,
    version UInt32,
    locktime UInt32,
    fee UInt64,
    input_count UInt16,
    output_count UInt16
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (node, network, block_height, txid)
ORDER BY (node, network, block_height, txid);
