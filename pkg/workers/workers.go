package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"crypto_options_screener/pkg/shared"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//------- РАЗОВЫЕ ФУНКЦИИ

// получает данные по всем монеткам для конкретной биржи и сохраняеет ее в БД
func GetAllSymbolsInfo(pool *pgxpool.Pool, exchange *shared.Exchange) {

	//для каждого символа для текущей биржи
	for symbolKey, symbolVal := range exchange.AllSymbols {

		//получим данные для текущей монетки
		symbolInfo, err := exchange.Client.GetSymbolInfo(symbolKey, exchange.FetchIntervals.RestDataGetInterval)
		if err != nil {
			slog.Error("Ошибка при получении", "exchangeName", exchange.ExchangeName, "symbol", symbolKey)
		}

		// slog.Info("Получены данные SymbolInfo", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "symbolInfo", symbolInfo)
		slog.Info("Получены данные SymbolInfo",
			"exchangeName", exchange.ExchangeName,
			"symbol", symbolKey,
			"IsSpotAvailable", symbolInfo.IsSpotAvailable,
			"IsPerpetualAvailable", symbolInfo.IsPerpetualAvailable,
			"IsFuturesAvailable", symbolInfo.IsFuturesAvailable,
			"IsOptionsAvailable", symbolInfo.IsOptionsAvailable,
		)

		//запишем данные в общую структуру
		symbolVal.Info.SaveInfo(symbolInfo)

		//запишем данные в бд
		// query := `
		// 	INSERT INTO all_symbols_info_new (
		// 		exchange_name, symbol,
		// 		spot_min_order_qty, spot_tick_size,
		// 		linear_min_order_qty, linear_tick_size,
		// 		option_min_order_qty, option_tick_size
		// 	)
		// 	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		// 	ON CONFLICT (exchange_name, symbol)
		// 	DO UPDATE SET
		// 		spot_min_order_qty = EXCLUDED.spot_min_order_qty,
		// 		spot_tick_size = EXCLUDED.spot_tick_size,
		// 		linear_min_order_qty = EXCLUDED.linear_min_order_qty,
		// 		linear_tick_size = EXCLUDED.linear_tick_size,
		// 		option_min_order_qty = EXCLUDED.option_min_order_qty,
		// 		option_tick_size = EXCLUDED.option_tick_size;
		// `

		// // 2. Безопасное выполнение запроса через пул соединений
		// _, err = pool.Exec(
		// 	context.Background(),
		// 	query,
		// 	symbolInfo.ExchangeName,
		// 	symbolInfo.Symbol,
		// 	symbolInfo.SpotMinOrderQty,
		// 	symbolInfo.SpotTickSize,
		// 	symbolInfo.LinearMinOrderQty,
		// 	symbolInfo.LinearTickSize,
		// 	symbolInfo.OptionMinOrderQty,
		// 	symbolInfo.OptionTickSize,
		// )
		// if err != nil {
		// 	slog.Error("Ошибка при сохранении данных в БД", "func", "GetAllSymbolsInfo", "exchangeName", exchange.ExchangeName, "symbol", exchange.AllSymbols[i].Symbol)
		// }

		//сделаем паузу между запросами
		time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
	}
}

// получает данные kline и сохраняет их в общем объекте
func GetKlineData(exchange *shared.Exchange, symbol string, contract string, category string, interval string) {

	//получим данные
	klines, err := exchange.Client.GetKline(contract, category, interval, "", "", exchange.KlineLimit)
	if err != nil {
		slog.Error("Ошибка при получении kline", "exchangeName", exchange.ExchangeName, "symbol", symbol, "contract", contract, "category", category, "interval", interval, "err", err)
		return
	}

	//если длина массива данных нулевая
	if len(klines) == 0 {
		slog.Warn("Длина данных len(klineData.Klines) == 0", "exchangeName", exchange.ExchangeName, "symbol", symbol, "contract", contract, "category", category, "interval", interval)
		return
	}

	//размернем данные klines - делаем жто сдеь чтобы в других функциях иметь возможность наккапливать исторические данные
	slices.Reverse(klines)

	//получим ключ
	key, err := exchange.Client.MakeKlineKey(contract, category, interval)
	if err != nil {
		slog.Error("Ошибка при получении ключа", exchange.ExchangeName, "symbol", symbol, "contract", contract, "category", category, "interval", interval, "err", err)
		return
	}
	exchange.AllKlineData.RLock()
	KlineData, ok := exchange.AllKlineData.KlineDataMap[key]
	exchange.AllKlineData.RUnlock()

	//если запись не существует
	if !ok {
		slog.Warn("Записи exchange.KlineData.KlineDataMap[key] не существует", "exchangeName", exchange.ExchangeName, "key", key)

		//создадим данные с таким ключом
		exchange.AllKlineData.Lock()

		//сделаем пустой объект с мьютексом
		exchange.AllKlineData.KlineDataMap[key] = &shared.KlineDataMu{}

		//заполним внутренний объект начальными значениями
		exchange.AllKlineData.KlineDataMap[key].KlineData = shared.KlineData{
			Symbol:   symbol,
			Contract: contract,
			Category: category,
			Interval: interval,

			Klines:     [][]string{},
			LastCandle: []string{},

			Ts: time.Now(), //запишем время получения

			DeleteTime: time.Now().Add(exchange.FetchIntervals.KlineDeleteDeltaTime),
		}
		exchange.AllKlineData.Unlock()

		// slog.Error("Текущее время", "time.Now()", time.Now())
		// slog.Error("Время удаления kline", "DeleteTime", time.Now().Add(exchange.FetchIntervals.KlineDeleteDeltaTime))

		//еще раз получим уже существующие данные
		KlineData, _ = exchange.AllKlineData.KlineDataMap[key]

		slog.Info("Создана запись exchange.KlineData.KlineDataMap[key]", "exchangeName", exchange.ExchangeName, "key", key)

	}

	//сохраним данные
	KlineData.Lock()
	//коопируем kline даже нулевой массив
	KlineData.KlineData.Klines = klines

	//если есть что копировать
	if len(klines) > 0 {
		//копируем последнюю свечу
		KlineData.KlineData.LastCandle = klines[len(klines)-1]
	}

	KlineData.Unlock()

}

// получает длинные данные kline и сохраняет их в бд
func GetKlineDataHistory(pool *pgxpool.Pool, table_name string, exchange *shared.Exchange, contract string, category string, interval string, total_length int) {
	var allKlines [][]string
	var currentEnd string

	for len(allKlines) < total_length {
		klines, err := exchange.Client.GetKline(contract, category, interval, "", currentEnd, exchange.KlineLimit)
		if err != nil {
			slog.Error("Ошибка при получении kline", "exchangeName", exchange.ExchangeName, "contract", contract, "category", category, "interval", interval, "err", err)
			return
		}

		if len(klines) == 0 {
			slog.Info("Достигнут конец доступной истории свечей", "exchangeName", exchange.ExchangeName, "contract", contract, "fetched", len(allKlines))
			break
		}

		// Самая старая свеча в пачке (последний индекс)
		oldestCandle := klines[len(klines)-1]
		if len(oldestCandle) == 0 {
			slog.Error("Некорректный формат свечи от биржи", "contract", contract)
			return
		}

		oldestTimestamp, err := strconv.ParseInt(oldestCandle[0], 10, 64)
		if err != nil {
			slog.Error("Ошибка парсинга timestamp свечи", "exchangeName", exchange.ExchangeName, "contract", contract, "timestamp", oldestCandle[0], "err", err)
			return
		}

		allKlines = append(allKlines, klines...)
		currentEnd = strconv.FormatInt(oldestTimestamp-1, 10)
		time.Sleep(exchange.FetchIntervals.HistoryKlinesDataGetInterval)
	}

	if len(allKlines) > total_length {
		allKlines = allKlines[:total_length]
	}

	if len(allKlines) == 0 {
		return
	}

	// Разворачиваем стандартным пакетом slices (от старых к новым)
	slices.Reverse(allKlines)

	type klineRecord struct {
		tsMs     int64
		openTime time.Time
		open     float64
		high     float64
		low      float64
		close    float64
	}

	// Конвертируем весь строковый массив в числовое представление
	parsedKlines := make([]klineRecord, 0, len(allKlines))

	for i, k := range allKlines {
		if len(k) < 5 {
			slog.Error("Некорректная длина массива свечи (ожидалось >= 5)", "exchangeName", exchange.ExchangeName, "contract", contract, "index", i, "kline", k)
			return
		}

		tsMs, err := strconv.ParseInt(k[0], 10, 64)
		if err != nil {
			slog.Error("Ошибка парсинга open_time", "exchangeName", exchange.ExchangeName, "contract", contract, "index", i, "val", k[0], "err", err)
			return
		}

		openPrice, err := strconv.ParseFloat(k[1], 64)
		if err != nil {
			slog.Error("Ошибка парсинга open", "exchangeName", exchange.ExchangeName, "contract", contract, "index", i, "val", k[1], "err", err)
			return
		}

		highPrice, err := strconv.ParseFloat(k[2], 64)
		if err != nil {
			slog.Error("Ошибка парсинга high", "exchangeName", exchange.ExchangeName, "contract", contract, "index", i, "val", k[2], "err", err)
			return
		}

		lowPrice, err := strconv.ParseFloat(k[3], 64)
		if err != nil {
			slog.Error("Ошибка парсинга low", "exchangeName", exchange.ExchangeName, "contract", contract, "index", i, "val", k[3], "err", err)
			return
		}

		closePrice, err := strconv.ParseFloat(k[4], 64)
		if err != nil {
			slog.Error("Ошибка парсинга close", "exchangeName", exchange.ExchangeName, "contract", contract, "index", i, "val", k[4], "err", err)
			return
		}

		parsedKlines = append(parsedKlines, klineRecord{
			tsMs:     tsMs,
			openTime: time.UnixMilli(tsMs).UTC(),
			open:     openPrice,
			high:     highPrice,
			low:      lowPrice,
			close:    closePrice,
		})
	}

	// Проверяем массив свечей на гладкость и отсутствие разрывов
	if len(parsedKlines) >= 2 {
		expectedDelta := parsedKlines[1].tsMs - parsedKlines[0].tsMs
		if expectedDelta <= 0 {
			slog.Error("Некорректный порядок или дубликат времени в первых свечах",
				"exchangeName", exchange.ExchangeName,
				"contract", contract,
				"ts0", parsedKlines[0].tsMs,
				"ts1", parsedKlines[1].tsMs,
			)
			return
		}

		for i := 1; i < len(parsedKlines); i++ {
			actualDelta := parsedKlines[i].tsMs - parsedKlines[i-1].tsMs
			if actualDelta != expectedDelta {
				slog.Error("Обнаружен разрыв в данных свечей",
					"exchangeName", exchange.ExchangeName,
					"contract", contract,
					"index", i,
					"prevTime", parsedKlines[i-1].openTime,
					"currTime", parsedKlines[i].openTime,
					"expectedDeltaMs", expectedDelta,
					"actualDeltaMs", actualDelta,
				)
				return
			}
		}
	}

	// Составляем батч из проверенного числового массива и отправляем в БД
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
		INSERT INTO %s (exchange, contract, open_time, open, high, low, close)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (exchange, contract, open_time) DO NOTHING
	`, pgx.Identifier{table_name}.Sanitize())

	batch := &pgx.Batch{}
	for _, rec := range parsedKlines {
		batch.Queue(query, exchange.ExchangeName, contract, rec.openTime, rec.open, rec.high, rec.low, rec.close)
	}

	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			slog.Error("Ошибка при вставке свечи в БД", "exchangeName", exchange.ExchangeName, "contract", contract, "table", table_name, "err", err)
			return
		}
	}

	slog.Info("Успешно загружена и сохранена история свечей", "exchangeName", exchange.ExchangeName, "contract", contract, "table", table_name, "count", len(parsedKlines))
}

//------- работа с БД------

// сохраняет данные тикеров для всех монет указанной биржи
func SaveAllTickersDataToDb(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

	//переменная для сохранения
	var tickerDataToSave shared.TickersDataToSave

	// Берем текущее время в UTC и отбрасываем лишние минуты, секунды и наносекунды до 15 минут
	ts := time.Now().UTC().Truncate(15 * time.Minute).UnixMilli()

	//для каждого символа для данной биржи
	for i := range exchange.AllSymbols {

		//получим данные для текущего символа
		exchange.AllSymbols[i].TickersData.Lock()
		tickerDataToSave.Spot = exchange.AllSymbols[i].TickersData.Spot
		tickerDataToSave.Perpetual = exchange.AllSymbols[i].TickersData.Perpetual
		tickerDataToSave.Futures = exchange.AllSymbols[i].TickersData.Futures
		tickerDataToSave.Options = exchange.AllSymbols[i].TickersData.Options
		tickerDataToSave.IndexPrice = exchange.AllSymbols[i].TickersData.IndexPrice
		// Преобразуем структуру TickersData в JSON ([]byte) для колонки JSONB - делаем внутри lock/unlock
		tickersJSON, err := json.Marshal(tickerDataToSave)
		if err != nil {
			slog.Error("Ошибка сериализации TickersData в JSON", "exchangeName", exchange.ExchangeName, "symbol", exchange.AllSymbols[i].Symbol, "err", err)
			exchange.AllSymbols[i].TickersData.Unlock()
			return
		}
		exchange.AllSymbols[i].TickersData.Unlock()

		query := `
			INSERT INTO all_tickers_data (
				exchange_name, symbol, tickers_data, ts
			)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (exchange_name, symbol, ts)
			DO UPDATE SET
				tickers_data = EXCLUDED.tickers_data;
		`

		_, err = pool.Exec(
			ctx,
			query,
			exchange.ExchangeName,
			exchange.AllSymbols[i].Symbol,
			tickersJSON,
			ts, // int64, полученный через .UnixMilli()
		)
		if err != nil {
			slog.Error("Ошибка при сохранении тикера в БД", "exchangeName", exchange.ExchangeName, "symbol", exchange.AllSymbols[i].Symbol, "err", err)
			return
		}

		slog.Info("Данные тикера сохранены в БД", "exchangeName", exchange.ExchangeName, "symbol", exchange.AllSymbols[i].Symbol)
	}
}

// получение записи из таблицы бд tickersData
func GetTickerDataFromDb(ctx context.Context, pool *pgxpool.Pool, exchangeName, symbol string, ts int64) (*shared.TickersDataToSave, error) {

	//выход
	var output shared.TickersDataToSave

	query := `
		SELECT tickers_data
		FROM all_tickers_data
		WHERE exchange_name = $1 AND symbol = $2 AND ts = $3;
	`

	var rawJSON []byte

	err := pool.QueryRow(ctx, query, exchangeName, symbol, ts).Scan(&rawJSON)
	if err != nil {
		// Если запись не найдена, pgx вернет pgx.ErrNoRows
		return nil, err
	}

	// Распаковываем JSONB обратно в Go-структуру
	if err := json.Unmarshal(rawJSON, &output); err != nil {
		return nil, err
	}

	return &output, nil
}

// ------- REST ВОРКЕРЫ ------

// получает данные с определенным интервалом и записывает их в бд
func GetDataSequentalTimer(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

	slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	//создадим тикер который каждые 1 секунду будет срабатывать
	ticker := time.NewTicker(1 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "exchangeName", exchange.ExchangeName)
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

			// на каждой n секунде
			if seconds%int(exchange.FetchIntervals.TickersDataGetInterval.Seconds()) == 0 {
				//последовательно получим данные
				exchange.Client.GetTickersDataSequental(exchange)

				//на каждоой 15 минуте сохраним данные в БД
				if minutes%15 == 0 {

					//соохраним данные тикеров для всех монет в бд
					SaveAllTickersDataToDb(ctx, pool, exchange)
				}
			}
			// на каждой 15 минуте и 10 секунде
			if (minutes%15 == 0) && seconds == int(exchange.FetchIntervals.TickersDataGetInterval.Seconds()) {

				//для каждого символа для текущей биржи
				for i := range exchange.AllSymbols {

					//потокобезопасно получим мап с данными kline
					exchange.AllKlineData.Lock()
					KlineDataMap := exchange.AllKlineData.KlineDataMap
					exchange.AllKlineData.Unlock()

					//для каждой записи kline
					for j := range KlineDataMap {

						//фильтр по интервалу
						if KlineDataMap[j].KlineData.Interval == "15m" {

							//получим и сохраним в общий объект
							GetKlineData(
								exchange,
								exchange.AllSymbols[i].Symbol,
								KlineDataMap[j].KlineData.Contract,
								KlineDataMap[j].KlineData.Category,
								KlineDataMap[j].KlineData.Interval,
							)
							//сделаем паузу между запросами
							time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
						}
					}
				}
			}
			//на каждой часовой минуте и 10 секунде
			if minutes == 0 && seconds == int(exchange.FetchIntervals.TickersDataGetInterval.Seconds()) {

				//для каждого символа для текущей биржи
				for i := range exchange.AllSymbols {

					//потокобезопасно получим мап с данными kline
					exchange.AllKlineData.Lock()
					KlineDataMap := exchange.AllKlineData.KlineDataMap
					exchange.AllKlineData.Unlock()

					//для каждого интервала - нужно чтобы интервалы шли последовательно
					for _, interval := range []string{"1h", "1D", "1W"} {

						//для каждой записи kline
						for j := range KlineDataMap {

							//фильтр по интервалу
							if KlineDataMap[j].KlineData.Interval == interval {

								//получим и сохраним в общий объект
								GetKlineData(
									exchange,
									exchange.AllSymbols[i].Symbol,
									KlineDataMap[j].KlineData.Contract,
									KlineDataMap[j].KlineData.Category,
									KlineDataMap[j].KlineData.Interval,
								)
								//сделаем паузу между запросами
								time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
							}
						}
					}
				}
			}

		//если работа программы остановлены
		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			slog.Info("Воркер остановлен", "exchangeName", exchange.ExchangeName)
			return
		}
	}
}

// воркер для сохранения данных в бд по таймеру
func SaveSymbolDataToDbTimer(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

	// slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	// //создадим тикер который будет срабатывать каждые 1 секунду
	// ticker := time.NewTicker(1 * time.Second)
	// defer func() {
	// 	ticker.Stop()
	// 	slog.Info("Таймер остановлен", "exchangeName", exchange.ExchangeName)
	// }()

	// for {
	// 	select {
	// 	//если тикер дал тик
	// 	case <-ticker.C:

	// 		//получим текущее время
	// 		now := time.Now().UTC()

	// 		//отбросим все до секунды
	// 		now = now.Truncate(time.Second)

	// 		//получим секунды
	// 		seconds := now.Second()

	// 		//получим минуты
	// 		minutes := now.Minute()

	// 		//если мы прошли отметку кратную 15 мин
	// 		if minutes%15 == 0 && seconds == 10 {

	// 			//для каждого символа - через индекс потому что внутри мьютексы
	// 			for symbolKey, symbolVal := range exchange.AllSymbols {

	// 				//сдвигаем время на 15 минут назад
	// 				fifteenMinutesAgo := now.Add(-15 * time.Minute)

	// 				//получаем миллисекунды (тип int64)
	// 				timeMs := fifteenMinutesAgo.UnixMilli()

	// 				//последняя цена спота
	// 				lastPriceSpot := "0"
	// 				if symbolVal.KlineSpot["15m"] != nil {
	// 					lastPriceSpot = symbolVal.KlineSpot["15m"].GetKlineLastCandleClosePrice()
	// 				}
	// 				//последняя цена фьючерса
	// 				lastPriceLinear := "0"
	// 				if symbolVal.KlineLinear["15m"] != nil {
	// 					lastPriceLinear = symbolVal.KlineLinear["15m"].GetKlineLastCandleClosePrice()
	// 				}

	// 				//сформируем объект для записи
	// 				symbolData := shared.SymbolData{
	// 					ExchangeName:    exchange.ExchangeName,
	// 					Symbol:          symbolVal.Symbol,
	// 					LastPriceSpot:   lastPriceSpot,
	// 					LastPriceLinear: lastPriceLinear,
	// 					LinearData:      []shared.LinearData{},
	// 					OptionsData:     symbolVal.OptionsData,
	// 					TimeMs:          strconv.FormatInt(timeMs, 10),
	// 				}

	// 				// 4. Обязательно переводим структуру Go в JSON-строку перед отправкой в БД
	// 				jsonData, err := json.Marshal(symbolData)
	// 				if err != nil {
	// 					slog.Error("Ошибка при создании json из symbolData", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "err", err)
	// 					return
	// 				}

	// 				// SQL-запрос для PostgreSQL (плейсхолдеры \$1, \$2...)
	// 				query := `
	// 					INSERT INTO all_symbol_data_new (exchange_name, symbol, symbol_data, time_ms)
	// 					VALUES ($1, $2, $3, $4);
	// 				`

	// 				// Создаем таймаут НА ОСНОВЕ переданного ctx. Если сработает сигнал остановки приложения ИЛИ пройдет 3 секунды — контекст закроется и pool выдаст ошибку
	// 				dbCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	// 				defer cancel()

	// 				// Выполняем запрос через пул соединений
	// 				_, err = pool.Exec(dbCtx, query, exchange.ExchangeName, symbolKey, string(jsonData), timeMs)
	// 				if err != nil {
	// 					slog.Error("Ошибка при сохранении SymbolData", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "timeMs", timeMs, "err", err)
	// 					return
	// 				}

	// 				slog.Info("Данные SymbolData сохранены в бд", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "timeMs", timeMs)
	// 			}
	// 		}

	// 	case <-ctx.Done():
	// 		// Завершаем работу функции по контексту - все defer отрабатают автоматически
	// 		slog.Info("Воркер остановлен", "exchangeName", exchange.ExchangeName)
	// 		return
	// 	}
	// }
}

//------- СИСТЕМНЫЕ ВОРКЕРЫ ------

// просматривает данные kline на всех биржах и монетах и отключает те время которых истекло
func ExpiredDataDeleteTimer(ctx context.Context, exchange *shared.Exchange, deltaTime time.Duration) {

	slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	//создадим тикер который каждые * секунду будет срабатывать
	ticker := time.NewTicker(deltaTime)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "exchangeName", exchange.ExchangeName)
	}()

	//основной цикл
	for {
		select {
		//если тикер дал тик
		case <-ticker.C:

			// fmt.Print("тик\n")

			//получим текущее время
			now := time.Now().UTC()

			//ключи для удаления
			var keysToDelete []string

			//блокируем мапу для чтения
			exchange.AllKlineData.RLock()
			for k, klineMu := range exchange.AllKlineData.KlineDataMap {
				klineMu.Lock()

				// slog.Error("Текущее время", "key", k, "time.Now()", time.Now())
				// slog.Error("Время уделаения", "key", k, "klineMu.KlineData.DeleteTime", klineMu.KlineData.DeleteTime)
				//проверим что текущее время больше записанного
				if now.After(klineMu.KlineData.DeleteTime) {
					// slog.Error("Время уделаения", "key", k, "klineMu.KlineData.DeleteTime", klineMu.KlineData.DeleteTime)

					//запишем ключ
					keysToDelete = append(keysToDelete, k)
				}
				klineMu.Unlock()
			}
			exchange.AllKlineData.RUnlock()

			//если есть ключи для удаления
			if len(keysToDelete) > 0 {

				//удалим записи kline
				exchange.AllKlineData.Lock()
				for i := range keysToDelete {
					delete(exchange.AllKlineData.KlineDataMap, keysToDelete[i])
					slog.Warn("Запись kline удалена по истечению времени", "keysToDelete[i]", keysToDelete[i])
				}
				exchange.AllKlineData.Unlock()

				//для каждого ключа
				for i := range keysToDelete {
					//разделим ключ
					keySplitted := strings.Split(keysToDelete[i], ":")
					//если это верный формат ключа
					if len(keySplitted) == 2 {
						category := keySplitted[0]
						topic := keySplitted[1]

						//для каждого вебсокета
						for wsKey := range exchange.Ws {
							ws := exchange.Ws[wsKey]
							//категория ключа для удаления и ключ вебсокета совпадают
							if wsKey == category {

								ws.Lock()
								// найдем индекс топика в списке топиков
								if idx := slices.Index(ws.TopicList, topic); idx != -1 {
									// Удаляем топик по найденному индексу
									ws.TopicList = slices.Delete(ws.TopicList, idx, idx+1)
									ws.Unlock()

									//сформируем команду
									cmdMsg := shared.CmdChanMsg{
										ID:         exchange.GenID.Next(),
										Action:     "unsubscribe",
										Topic:      topic,
										DeleteTime: time.Now().Add(exchange.FetchIntervals.WsCmdMsgDeleteDelta),
									}

									// Отправляем команду в канал управления ws
									ws.CmdChan <- cmdMsg

									slog.Warn("Отправлена команда unsubscribe", "cmdMsg", cmdMsg)

									break
								}
								ws.Unlock()
							}
						}
					}
				}
			}

			//для каждого вебсокета
			for i := range exchange.Ws {
				ws := exchange.Ws[i]
				ws.Lock()
				//для каждой команды
				var deleteIndexList []int
				for j := range ws.CmdBuffer {
					//есл текущее время больше времени удаления
					if now.After(ws.CmdBuffer[j].DeleteTime) {
						deleteIndexList = append(deleteIndexList, j)
					}
				}

				// удалим элементы в обратном порядке
				for _, idx := range slices.Backward(deleteIndexList) {
					// Быстрое удаление со сдвигом элементов (сохраняет порядок в буфере)
					ws.CmdBuffer = append(ws.CmdBuffer[:idx], ws.CmdBuffer[idx+1:]...)
				}
				ws.Unlock()

			}

		//если работа программы остановлены
		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			slog.Info("Воркер остановлен")
			return
		}
	}
}

// воркер раз в * сек показывает размер объекта
func GetMemStatsTimer(ctx context.Context, exchange *shared.Exchange, deltaTime time.Duration) {

	slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	//создадим тикер который каждые 10 секунд будет срабатывать
	ticker := time.NewTicker(deltaTime)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "exchangeName", exchange.ExchangeName)
	}()

	for {
		select {
		//если тикер дал тик
		case <-ticker.C:

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			slog.Warn("Метрики памяти", "Alloc_MB", float64(m.Alloc)/1024/1024, "Sys_MB", float64(m.Sys)/1024/1024, "HeapObjects", m.HeapObjects)

		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			slog.Info("Воркер остановлен", "exchangeName", exchange.ExchangeName)
			return
		}
	}
}

// воркер раз в * сек показывает состояние выбранных параметров биржи
func GetExangeStateTimer(ctx context.Context, exchange *shared.Exchange, deltaTime time.Duration) {

	slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	//создадим тикер который каждые * секунд будет срабатывать
	ticker := time.NewTicker(deltaTime)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "exchangeName", exchange.ExchangeName)
	}()

	for {
		select {
		//если тикер дал тик
		case <-ticker.C:

			var KlineDataMapKeys []string

			exchange.AllKlineData.RLock()
			KlineDataMap := exchange.AllKlineData.KlineDataMap
			for key := range KlineDataMap {
				KlineDataMapKeys = append(KlineDataMapKeys, key)
			}
			exchange.AllKlineData.RUnlock()

			slog.Debug("Запрашиваемые Klines", "ExchangeName", exchange.ExchangeName, "KlineDataMapKeys", KlineDataMapKeys)

			//для подключенных вебсокетов
			for baseEndPoint := range exchange.Ws {

				ws := exchange.Ws[baseEndPoint]

				ws.Lock()
				topicListCopy := make([]string, len(ws.TopicList))
				copy(topicListCopy, ws.TopicList)
				cmdBufferCopy := make([]shared.CmdChanMsg, len(ws.CmdBuffer))
				copy(cmdBufferCopy, ws.CmdBuffer)
				ws.Unlock()

				slog.Debug("Состояние ws", "ExchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "TopicList", topicListCopy, "CmdBuffer", cmdBufferCopy)

			}

		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			slog.Info("Воркер остановлен", "exchangeName", exchange.ExchangeName)
			return
		}
	}
}
