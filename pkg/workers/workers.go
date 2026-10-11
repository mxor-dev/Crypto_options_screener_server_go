package workers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"crypto_options_screener/pkg/shared"

	"github.com/jackc/pgx/v5/pgxpool"
)

// получает данные по всем монеткам для конкретной биржи и сохраняеет ее в БД
func GetAllSymbolsInfo(exchange shared.Exchange, pool *pgxpool.Pool) {

	//для каждого символа для текущей биржи
	for _, symbol := range exchange.Symbols {

		fmt.Printf("Symbol=%s\n", symbol)

		//получим данные для текущей монетки
		symbolInfo, err := exchange.Client.GetSymbolInfo(symbol)
		if err != nil {
			slog.Error("Ошибка при получении symbolInfo", "exchangeName", exchange.Name, "symbol", symbol)
		}

		slog.Info("Получен symbolInfo", "exchangeName", exchange.Name, "symbol", symbol, "symbolInfo", symbolInfo)

		//запишем данные в бд
		query := `
			INSERT INTO all_symbols_info (
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
			slog.Error("Не удалось сохранить символ в базу данных",
				"exchange", symbolInfo.ExchangeName,
				"symbol", symbolInfo.Symbol,
				"error", err,
			)
		}

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}

// получает разово данные kline для всех монет и всех интервалов и сохранеет их в БД
func GetKlineDataInitial(exchange shared.Exchange, pool *pgxpool.Pool, allKlineIntervals []string) {

	//для каждого символа для текущей биржи
	for _, symbol := range exchange.Symbols {

		klineData, err := exchange.Client.GetKline(symbol, "spot", "15m", "1000")
		if err != nil {
			slog.Error("Ошибка при получении kline", "exchangeName", exchange.Name, "symbol", symbol)
		}
		// client.GetKline(symbol, "linear", "15m")

		slog.Info("Получены данные kline", "exchangeName", exchange.Name, "symbol", symbol, "len(klineData.KlineData)", len(klineData.KlineData))

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}
