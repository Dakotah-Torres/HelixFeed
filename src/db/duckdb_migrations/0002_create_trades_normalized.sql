CREATE TABLE IF NOT EXISTS  trades_normalized (
    time_stamp TIMESTAMPTZ,
    data_provider TEXT,
    asset TEXT,
    price DECIMAL(20,8),
    volume DECIMAL(20,10), 
    side TEXT,
    CHECK (side IN ('Bid', 'Ask'))
)

