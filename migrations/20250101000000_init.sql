-- Up migration: Creates tables and initializes the net-flow record

-- 1. Table for Raw Transaction Data
CREATE TABLE raw_pol_transfers (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_hash        TEXT    NOT NULL,
    log_index               INTEGER NOT NULL,
    block_number            INTEGER NOT NULL,
    block_timestamp         INTEGER NOT NULL,
    from_address            TEXT    NOT NULL,
    to_address              TEXT    NOT NULL,
    amount_wei              TEXT    NOT NULL,
    is_in_flow              INTEGER NOT NULL,
    is_out_flow             INTEGER NOT NULL,
    UNIQUE (transaction_hash, log_index)
);
CREATE INDEX idx_raw_pol_transfers_block_number ON raw_pol_transfers (block_number);


-- 2. Table for the Final Processed Metric (Cumulative Net-Flows)
CREATE TABLE cumulative_net_flows (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange_name            TEXT    NOT NULL,
    token_symbol             TEXT    NOT NULL,
    latest_block_indexed     INTEGER NOT NULL,
    cumulative_net_flow_wei  TEXT    NOT NULL,
    updated_at               TEXT    NOT NULL,
    UNIQUE (exchange_name, token_symbol)
);

-- 3. Initial Data Insertion (Deliverable 6: Indexing starts from block 0)
INSERT INTO cumulative_net_flows (exchange_name, token_symbol, latest_block_indexed, cumulative_net_flow_wei, updated_at)
VALUES ('Binance', 'POL', 0, '0', datetime('now'))
ON CONFLICT(exchange_name, token_symbol) DO UPDATE SET latest_block_indexed=excluded.latest_block_indexed;