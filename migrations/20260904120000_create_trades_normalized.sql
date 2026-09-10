-- Add migration script here
CREATE TABLE trades_normalized (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    trade_id BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received TIMESTAMPTZ NOT NULL,
    provider_meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_trades_normalized_symbol_provider_time
    ON trades_normalized (symbol, provider, event_time);
