CREATE TABLE all_tickers_data (
    id SERIAL PRIMARY KEY,
    exchange_name VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    tickers_data JSONB NOT NULL,
    ts BIGINT NOT NULL,
    CONSTRAINT unique_all_tickers_data UNIQUE (exchange_name, symbol, ts)
);

CREATE TABLE kline_data_15m (
    exchange TEXT NOT NULL,
    contract TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (exchange, contract, open_time)
);