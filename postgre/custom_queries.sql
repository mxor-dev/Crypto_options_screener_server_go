SELECT id, symbol, time_stamp, json_data
FROM all_symbols_data 
WHERE symbol = 'BTC';




SELECT id, symbol, series, min_iv_line 
FROM all_iv_data 
WHERE symbol = 'BTC' AND series = '17JUL26';