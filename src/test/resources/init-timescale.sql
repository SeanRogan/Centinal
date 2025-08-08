-- Initialize TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create market_data table with time-series optimization
CREATE TABLE IF NOT EXISTS market_data (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    price DECIMAL(20,8),
    volume DECIMAL(20,8),
    bid DECIMAL(20,8),
    ask DECIMAL(20,8),
    high_24h DECIMAL(20,8),
    low_24h DECIMAL(20,8),
    open_24h DECIMAL(20,8),
    raw_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create indexes for time-series queries
CREATE INDEX IF NOT EXISTS idx_market_data_timestamp ON market_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_data_symbol ON market_data (symbol);
CREATE INDEX IF NOT EXISTS idx_market_data_exchange ON market_data (exchange);
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp ON market_data (symbol, timestamp DESC);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('market_data', 'timestamp', if_not_exists => TRUE);

-- Set chunk time interval (1 day chunks for testing)
SELECT set_chunk_time_interval('market_data', INTERVAL '1 day'); 