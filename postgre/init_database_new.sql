

CREATE TABLE all_symbol_data_new (
    exchange_name VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    symbol_data TEXT NOT NULL, 
    time_ms BIGINT NOT NULL,  

    PRIMARY KEY (exchange_name, symbol, time_ms)
);