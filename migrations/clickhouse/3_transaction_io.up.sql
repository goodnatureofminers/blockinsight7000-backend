CREATE TABLE IF NOT EXISTS btc_transaction_inputs (
    node LowCardinality(String),
    network LowCardinality(String),
    txid FixedString(64),
    input_index UInt32,
    prev_txid FixedString(64),
    prev_vout UInt32,
    sequence UInt32,
    is_coinbase UInt8,
    value UInt64,
    script_sig_hex String,
    script_sig_asm String,
    witness Array(String),
    addresses Array(String)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (node, network, txid, input_index)
ORDER BY (node, network, txid, input_index);

CREATE TABLE IF NOT EXISTS btc_transaction_outputs (
    node LowCardinality(String),
    network LowCardinality(String),
    txid FixedString(64),
    output_index UInt32,
    value UInt64,
    script_type LowCardinality(String),
    script_hex String,
    script_asm String,
    addresses Array(String)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (node, network, txid, output_index)
ORDER BY (node, network, txid, output_index);
