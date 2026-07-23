-- Add migration script here
CREATE TABLE  trades_raw (
    id SERIAL PRIMARY KEY, 
    received TIMESTAMPTZ,
    data_provider TEXT,
    symbol TEXT,
    raw_json JSONB
)