CREATE TABLE all_tickers_data (
    id SERIAL PRIMARY KEY,
    exchange_name VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    tickers_data JSONB NOT NULL,
    ts BIGINT NOT NULL,
    CONSTRAINT unique_all_tickers_data UNIQUE (exchange_name, symbol, ts)
);