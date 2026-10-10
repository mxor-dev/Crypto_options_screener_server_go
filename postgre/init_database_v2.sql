CREATE TABLE all_symbols_info_v2 (
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