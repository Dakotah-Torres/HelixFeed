CREATE TABLE IF NOT EXISTS _migrations (
    version VARCHAR PRIMARY KEY,
    applied_at BIGINT NOT NULL
);