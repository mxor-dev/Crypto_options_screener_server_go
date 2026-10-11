package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"crypto_options_screener/pkg/exchanges/exchange_client"
	"crypto_options_screener/pkg/shared"

	"github.com/jackc/pgx/v5/pgxpool"
)

//------- СОХРАНЕНИЕ ДАННЫХ БД

// сохраняет данные kline в бд
func SaveKlineToDb(klineData exchange_client.KlineData, pool *pgxpool.Pool) {

	// Переводим двумерный массив [][]string в JSON-строку
	jsonData, err := json.Marshal(klineData.KlineData)
	if err != nil {
		slog.Error("Ошибка при создании json из kline", "func", "SaveKlineToDb", "exchangeName", klineData.ExchangeName, "symbol", klineData.Symbol, "err", err)
	}

	// 2. Пишем SQL-запрос с плейсхолдерами
	query := `
		INSERT INTO all_kline_data_buffer_new (
			exchange_name, 
			symbol, 
			category, 
			kline_interval, 
			kline_data, 
			time_ms
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (exchange_name, symbol, category, kline_interval) 
		DO UPDATE SET 
			kline_data = EXCLUDED.kline_data,
			time_ms = EXCLUDED.time_ms;
	`

	// 3. Выполняем запрос через пул соединений
	_, err = pool.Exec(
		context.Background(),
		query,
		klineData.ExchangeName,
		klineData.Symbol,
		klineData.Category,
		klineData.KlineInterval,
		string(jsonData),
		klineData.Time,
	)
	if err != nil {
		slog.Error("Ошибка при сохранении данных в БД", "func", "SaveKlineToDb", "exchangeName", klineData.ExchangeName, "symbol", klineData.Symbol, "err", err)
	}

}

//------- ПОЛУЧЕНИЕ ДАННЫХ С БИРЖ

// получает данные по всем монеткам для конкретной биржи и сохраняеет ее в БД
func GetAllSymbolsInfo(exchange shared.Exchange, pool *pgxpool.Pool) {

	//для каждого символа для текущей биржи
	for _, symbol := range exchange.Symbols {

		fmt.Printf("Symbol=%s\n", symbol)

		//получим данные для текущей монетки
		symbolInfo, err := exchange.Client.GetSymbolInfo(symbol)
		if err != nil {
			slog.Error("Ошибка при получении", "func", "GetAllSymbolsInfo", "exchangeName", exchange.Name, "symbol", symbol)
		}

		slog.Info("Получен symbolInfo", "exchangeName", exchange.Name, "symbol", symbol, "symbolInfo", symbolInfo)

		//запишем данные в бд
		query := `
			INSERT INTO all_symbols_info_new (
				exchange_name, symbol, 
				spot_min_order_qty, spot_tick_size, 
				linear_min_order_qty, linear_tick_size, 
				option_min_order_qty, option_tick_size
			) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (exchange_name, symbol) 
			DO UPDATE SET 
				spot_min_order_qty = EXCLUDED.spot_min_order_qty,
				spot_tick_size = EXCLUDED.spot_tick_size,
				linear_min_order_qty = EXCLUDED.linear_min_order_qty,
				linear_tick_size = EXCLUDED.linear_tick_size,
				option_min_order_qty = EXCLUDED.option_min_order_qty,
				option_tick_size = EXCLUDED.option_tick_size;
		`

		// 2. Безопасное выполнение запроса через пул соединений
		_, err = pool.Exec(
			context.Background(),
			query,
			symbolInfo.ExchangeName,
			symbolInfo.Symbol,
			symbolInfo.SpotMinOrderQty,
			symbolInfo.SpotTickSize,
			symbolInfo.LinearMinOrderQty,
			symbolInfo.LinearTickSize,
			symbolInfo.OptionMinOrderQty,
			symbolInfo.OptionTickSize,
		)
		if err != nil {
			slog.Error("Ошибка при сохранении данных в БД", "func", "GetAllSymbolsInfo", "exchangeName", exchange.Name, "symbol", symbol)
		}

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}

// получает разово данные kline для всех монет и всех интервалов и сохранеет их в БД
func GetKlineDataInitial(exchange shared.Exchange, pool *pgxpool.Pool, allKlineIntervals []string) {

	//для каждого символа для текущей биржи
	for _, symbol := range exchange.Symbols {

		//для каждого интервала
		for _, interval := range allKlineIntervals {

			//получим данные
			// klineData, err := exchange.Client.GetKline(symbol, "spot", interval, "1000")
			klineData, err := exchange.Client.GetKline(symbol, "linear", interval, "1000")

			if err != nil {
				slog.Error("Ошибка при получении kline", "exchangeName", exchange.Name, "symbol", symbol, "err", err)
			}

			slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", symbol, "len(klineData.KlineData)", len(klineData.KlineData))

			//сохранение данных в бд
			SaveKlineToDb(klineData, pool)
		}

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}
