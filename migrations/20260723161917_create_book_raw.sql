-- Add migration script here
CREATE TABLE  book_raw (
    id SERIAL PRIMARY KEY, 
    received TIMESTAMPTZ,
    data_provider TEXT,
    symbol TEXT,
    raw_json JSONB
)