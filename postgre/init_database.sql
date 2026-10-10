CREATE TABLE all_symbols_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    time_stamp BIGINT NOT NULL,
    json_data JSONB NOT NULL,
    date_string CHAR(10) NOT NULL,
    time_string CHAR(5) NOT NULL,
    CONSTRAINT unique_symbol_time_stamp UNIQUE (symbol, time_stamp)
);

CREATE TABLE all_iv_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    series VARCHAR(7) NOT NULL,
    min_iv_line JSONB NOT NULL,
    CONSTRAINT unique_symbol_series UNIQUE (symbol, series)
);

CREATE TABLE all_symbols_data_buffer (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    time_stamp BIGINT NOT NULL,
    json_data JSONB NOT NULL,
    date_string CHAR(10) NOT NULL,
    time_string CHAR(5) NOT NULL,
    CONSTRAINT unique_all_symbols_data_buffer UNIQUE (symbol)
);

CREATE TABLE all_kline_data_buffer (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    kline_interval VARCHAR(2) NOT NULL,
    date_time VARCHAR(20) NOT NULL,
    json_data JSONB NOT NULL,
    CONSTRAINT unique_all_kline_data_buffer UNIQUE (symbol, kline_interval)
);

CREATE TABLE all_symbols_info (
    exchange_name        VARCHAR(20) NOT NULL,
    symbol               VARCHAR(20) NOT NULL,
    
    spot_min_order_qty   VARCHAR(50),
    spot_tick_size       VARCHAR(50),
    
    linear_min_order_qty VARCHAR(50),
    linear_tick_size     VARCHAR(50),
    
    option_min_order_qty VARCHAR(50),
    option_tick_size     VARCHAR(50),
    
    PRIMARY KEY (exchange_name, symbol)
);
