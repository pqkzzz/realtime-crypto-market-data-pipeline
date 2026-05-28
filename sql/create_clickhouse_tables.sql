CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.trades_clean
(
    event_time DateTime64(3),
    trade_time DateTime64(3),
    symbol String,
    trade_id UInt64,
    price Float64,
    quantity Float64,
    trade_value Float64,
    is_buyer_market_maker UInt8,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (symbol, trade_time, trade_id);

CREATE TABLE IF NOT EXISTS crypto.dead_letter_events
(
    event_time DateTime DEFAULT now(),
    raw_event String,
    error_message String
)
ENGINE = MergeTree
ORDER BY event_time;