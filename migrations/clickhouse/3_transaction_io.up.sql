CREATE TABLE IF NOT EXISTS btc_transaction_inputs (
    network LowCardinality(String) CODEC(ZSTD(1)),
    block_height UInt32 CODEC(ZSTD(1)),
    block_timestamp DateTime('UTC') CODEC(Delta(4), LZ4),
    txid FixedString(64) CODEC(ZSTD(1)),
    input_index UInt32 CODEC(ZSTD(1)),
    prev_txid FixedString(64) CODEC(ZSTD(1)),
    prev_vout UInt32 CODEC(ZSTD(1)),
    sequence UInt32 CODEC(ZSTD(1)),
    is_coinbase UInt8 CODEC(ZSTD(1)),
    value UInt64 CODEC(ZSTD(1)),
    script_sig_hex String CODEC(ZSTD(3)),
    script_sig_asm String CODEC(ZSTD(3)),
    witness Array(String) CODEC(ZSTD(3)),
    addresses Array(String) CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree
PARTITION BY (network,toYYYYMM(block_timestamp))
PRIMARY KEY (network, block_height, txid, input_index)
ORDER BY (network, block_height, txid, input_index);

CREATE TABLE IF NOT EXISTS btc_transaction_outputs (
    network LowCardinality(String) CODEC(ZSTD(1)),
    block_height UInt32 CODEC(ZSTD(1)),
    block_timestamp DateTime('UTC') CODEC(Delta(4), LZ4),
    txid FixedString(64) CODEC(ZSTD(1)),
    output_index UInt32 CODEC(ZSTD(1)),
    value UInt64 CODEC(ZSTD(1)),
    script_type LowCardinality(String) CODEC(ZSTD(1)),
    script_hex String CODEC(ZSTD(3)),
    script_asm String CODEC(ZSTD(3)),
    addresses Array(String) CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree
PARTITION BY (network, toYYYYMM(block_timestamp))
PRIMARY KEY (network, block_height, txid, output_index)
ORDER BY (network, block_height, txid, output_index);
