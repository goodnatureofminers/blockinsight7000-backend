CREATE TABLE IF NOT EXISTS utxo_transaction_inputs (
    coin LowCardinality(String) CODEC(ZSTD(1)),
    network LowCardinality(String) CODEC(ZSTD(1)),
    block_height UInt64 CODEC(ZSTD(1)),
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
    addresses Array(String) CODEC(ZSTD(3)),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(4), LZ4)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (coin, network)
ORDER BY (coin, network, block_height, txid, input_index);


CREATE TABLE IF NOT EXISTS utxo_transaction_outputs
(
    coin LowCardinality(String) CODEC(ZSTD(1)),
    network LowCardinality(String) CODEC(ZSTD(1)),
    block_height UInt64 CODEC(ZSTD(1)),
    txid FixedString(64) CODEC(ZSTD(1)),
    output_index UInt32 CODEC(ZSTD(1)),
    value UInt64 CODEC(ZSTD(1)),
    script_type LowCardinality(String) CODEC(ZSTD(1)),
    script_hex String CODEC(ZSTD(3)),
    script_asm String CODEC(ZSTD(3)),
    addresses Array(String) CODEC(ZSTD(3)),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(4), LZ4)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (coin, network)
ORDER BY (coin, network, block_height, txid, output_index);

CREATE TABLE IF NOT EXISTS utxo_transaction_outputs_lookup
(
    coin LowCardinality(String) CODEC(ZSTD(1)),
    network LowCardinality(String) CODEC(ZSTD(1)),
    txid FixedString(64) CODEC(ZSTD(1)),
    output_index UInt32 CODEC(ZSTD(1)),
    value UInt64 CODEC(ZSTD(1)),
    addresses Array(String) CODEC(ZSTD(3)),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(4), LZ4),

    INDEX idx_txid txid TYPE bloom_filter(0.01) GRANULARITY 4
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY (coin, network)
    ORDER BY (coin, network, txid, output_index);