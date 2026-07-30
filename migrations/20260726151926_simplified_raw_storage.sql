-- Add migration script here
DROP TABLE IF EXISTS trades_raw; 
DROP TABLE IF EXISTS book_raw; 

CREATE TABLE  raw_financial_data (
    id SERIAL PRIMARY KEY, 
    received TIMESTAMPTZ,
    data_provider TEXT,
    data_type TEXT,
    symbol TEXT,
    raw_json JSONB
)