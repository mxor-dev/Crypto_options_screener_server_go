CREATE TABLE all_symbols_info_new (
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


CREATE TABLE all_kline_data_buffer_new (
    exchange_name VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    category VARCHAR(20) NOT NULL,
    kline_interval VARCHAR(10) NOT NULL,
    kline_data TEXT NOT NULL, 
    time_ms BIGINT NOT NULL,  

    PRIMARY KEY (exchange_name, symbol, category, kline_interval)
);