package workers

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"time"

	"crypto_options_screener/pkg/exchanges/exchange_client"
	"crypto_options_screener/pkg/shared"

	"github.com/jackc/pgx/v5/pgxpool"
)

//------- СОХРАНЕНИЕ ДАННЫХ БД

// сохраняет данные kline в бд
func SaveKlineToDb(pool *pgxpool.Pool, klineData exchange_client.KlineData) {

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
func GetAllSymbolsInfo(pool *pgxpool.Pool, exchange shared.Exchange) {

	//для каждого символа для текущей биржи
	for _, symbol := range exchange.Symbols {

		//получим данные для текущей монетки
		symbolInfo, err := exchange.Client.GetSymbolInfo(symbol.Symbol)
		if err != nil {
			slog.Error("Ошибка при получении", "func", "GetAllSymbolsInfo", "exchangeName", exchange.Name, "symbol", symbol.Symbol)
		}

		slog.Info("Получен symbolInfo", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "symbolInfo", symbolInfo)

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
			slog.Error("Ошибка при сохранении данных в БД", "func", "GetAllSymbolsInfo", "exchangeName", exchange.Name, "symbol", symbol.Symbol)
		}

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}

// получает разово данные kline для всех монет и всех интервалов и сохранеет их в БД
func GetKlineDataInitial(pool *pgxpool.Pool, exchange shared.Exchange, allKlineIntervals []string) {

	//для каждого символа для текущей биржи
	for _, symbol := range exchange.Symbols {

		//для каждого интервала
		for _, interval := range allKlineIntervals {

			//если для символа стоит отметка получать данные spot
			if slices.Contains(symbol.DataToGet, "spot") {

				//получим данные
				klineData, err := exchange.Client.GetKline(symbol.Symbol, "spot", interval, "1000")
				if err != nil {
					slog.Error("Ошибка при получении kline spot", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "err", err)
				}

				slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

				//сохранение данных в бд
				SaveKlineToDb(pool, klineData)
			}

			if slices.Contains(symbol.DataToGet, "linear") {

				//получим данные
				klineData, err := exchange.Client.GetKline(symbol.Symbol, "linear", interval, "1000")
				if err != nil {
					slog.Error("Ошибка при получении kline linear", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "err", err)
				}

				slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

				//сохранение данных в бд
				SaveKlineToDb(pool, klineData)
			}

		}

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}

// получает данные с определенным интервалом и записывает их в бд
func GetKlineDataTimer(ctx context.Context, pool *pgxpool.Pool, exchange shared.Exchange) {

	slog.Info("Запуск воркера", "func", "GetKlineDataTimer", "exchangeName", exchange.Name)

	//создадим тикер который каждые 1 секунду будет срабатывать
	ticker := time.NewTicker(1 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Тикер для получения данных kline остановлен", "func", "GetKlineDataTimer", "exchangeName", exchange.Name)

	}()

	//переменная для остановке

	for {
		select {
		//если тикер дал тик
		case <-ticker.C:

			// fmt.Print("тик\n")

			//получим текущее время
			now := time.Now().UTC()

			//отбросим все до секунды
			now = now.Truncate(time.Second)

			//получим секунды
			seconds := now.Second()

			//получим минуты
			minutes := now.Minute()

			// на каждой 15 минуте и 10 секунде
			if (minutes == 0 || minutes == 15 || minutes == 30 || minutes == 45) && seconds == 10 {

				//для каждого символа для текущей биржи
				for _, symbol := range exchange.Symbols {

					//для каждого интервала
					for _, interval := range []string{"15m"} {

						//если для символа стоит отметка получать данные spot
						if slices.Contains(symbol.DataToGet, "spot") {

							//получим данные
							klineData, err := exchange.Client.GetKline(symbol.Symbol, "spot", interval, "1000")
							if err != nil {
								slog.Error("Ошибка при получении kline spot", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//сохранение данных в бд
							SaveKlineToDb(pool, klineData)
						}

						if slices.Contains(symbol.DataToGet, "linear") {

							//получим данные
							klineData, err := exchange.Client.GetKline(symbol.Symbol, "linear", interval, "1000")
							if err != nil {
								slog.Error("Ошибка при получении kline linear", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//сохранение данных в бд
							SaveKlineToDb(pool, klineData)
						}

					}

					//сделаем паузу между запросами
					time.Sleep(exchange.Intervals.RestDataGetInterval)
				}

			}
			//на каждой часовой минуте и 10 секунде
			if minutes == 0 && seconds == 10 {

				//для каждого символа для текущей биржи
				for _, symbol := range exchange.Symbols {

					//для каждого интервала
					for _, interval := range []string{"1h", "1D", "1W"} {

						//если для символа стоит отметка получать данные spot
						if slices.Contains(symbol.DataToGet, "spot") {

							//получим данные
							klineData, err := exchange.Client.GetKline(symbol.Symbol, "spot", interval, "1000")
							if err != nil {
								slog.Error("Ошибка при получении kline spot", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//сохранение данных в бд
							SaveKlineToDb(pool, klineData)
						}

						if slices.Contains(symbol.DataToGet, "linear") {

							//получим данные
							klineData, err := exchange.Client.GetKline(symbol.Symbol, "linear", interval, "1000")
							if err != nil {
								slog.Error("Ошибка при получении kline linear", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", symbol.Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//сохранение данных в бд
							SaveKlineToDb(pool, klineData)
						}

					}

					//сделаем паузу между запросами
					time.Sleep(exchange.Intervals.RestDataGetInterval)
				}

			}

		//если работа программы остановлены
		case <-ctx.Done():
			// Завершаем работу по контексту
			return
		}
	}

	//массив для конвертации интервала
	// intervalsTime := map[string]time.Time{
	// // 	"5m":  5 * time.Minute,
	// // 	"15m": 15 * time.Minute,
	// // 	"1h":  1 * time.Hour,
	// // 	"1D":  "D",
	// // 	"1W":  "W",
	// // }

}
