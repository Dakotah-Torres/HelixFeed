-- Add migration script here
CREATE TABLE orders_normalized (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received TIMESTAMPTZ NOT NULL,
    checksum BIGINT NOT NULL,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL,
    provider_meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_orders_normalized_symbol_provider_time
    ON orders_normalized (symbol, provider, event_time);
