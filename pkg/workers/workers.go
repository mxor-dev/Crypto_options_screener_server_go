package workers

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"time"

	"crypto_options_screener/pkg/shared"

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
func GetKlineData(exchange *shared.Exchange, symbol string, contract string, category string, interval string, limit string, KlineDeleteDeltaTime time.Duration) {

	//получим данные
	klines, err := exchange.Client.GetKline(contract, category, interval, limit)
	if err != nil {
		slog.Error("Ошибка при получении kline", "exchangeName", exchange.ExchangeName, "symbol", symbol, "contract", contract, "category", category, "interval", interval, "err", err)
		return
	}

	//если длина массива данных нулевая
	if len(klines) == 0 {
		slog.Warn("Длина данных len(klineData.Klines) == 0", "exchangeName", exchange.ExchangeName, "symbol", symbol, "contract", contract, "category", category, "interval", interval)
		return
	}
	//получим ключ
	key := fmt.Sprintf("%s:%s:%s", contract, category, interval)
	exchange.AllSymbols[symbol].AllKlineData.RLock()
	KlineData, ok := exchange.AllSymbols[symbol].AllKlineData.KlineDataMap[key]
	exchange.AllSymbols[symbol].AllKlineData.RUnlock()

	//если запись не существует
	if !ok {
		slog.Warn("Записи exchange.AllSymbols[symbol].KlineData.KlineDataMap[key] не существует", "exchangeName", exchange.ExchangeName, "symbol", symbol, "contract", contract, "category", category, "interval", interval)

		//создадим данные с таким ключом
		exchange.AllSymbols[symbol].AllKlineData.Lock()

		//сделаем пустой объект с мьютексом
		exchange.AllSymbols[symbol].AllKlineData.KlineDataMap[key] = &shared.KlineDataMu{}

		//заполним внутренний объект начальными значениями
		exchange.AllSymbols[symbol].AllKlineData.KlineDataMap[key].KlineData = shared.KlineData{
			Contract: contract,
			Category: category,
			Interval: interval,

			Klines:     [][]string{},
			LastCandle: []string{},

			DeleteTime: time.Now().Add(KlineDeleteDeltaTime),
		}
		exchange.AllSymbols[symbol].AllKlineData.Unlock()

		//еще раз получим уже существующие данные
		KlineData, _ = exchange.AllSymbols[symbol].AllKlineData.KlineDataMap[key]

		slog.Info("Создана запись exchange.AllSymbols[symbol].KlineData.KlineDataMap[key]", "exchangeName", exchange.ExchangeName, "symbol", symbol, "contract", contract, "category", category, "interval", interval)

	}

	//сохраним данные
	KlineData.SaveKlines(klines)

}

// получает разово данные kline для всех монет и всех интервалов и сохранеет их в БД
// func GetKlineDataInitial(exchange *shared.Exchange, allKlineIntervals []string, symbol string, contract string, category string, limit string) {

// 	slog.Debug("Данные интервалов", "allKlineIntervals", allKlineIntervals)
// 	slog.Debug("Пауза между запросами", "exchange.FetchIntervals.RestDataGetInterval", exchange.FetchIntervals.RestDataGetInterval)

// 	//для каждого интервала
// 	for i := range allKlineIntervals {
// 		slog.Debug("Интервал", "i", i)
// 		//получим данные и сохраним их
// 		GetKlineData(exchange, symbol, contract, category, allKlineIntervals[i], limit)

// 		//сделаем паузу между запросами
// 		time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
// 	}

// }

//------- REST ВОРКЕРЫ ------

// получает данные с определенным интервалом и записывает их в бд
func GetKlineDataTimer(ctx context.Context, exchange *shared.Exchange, KlineDeleteDeltaTime time.Duration) {

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

			// на каждой 15 минуте и 10 секунде
			if (minutes%15 == 0) && seconds == 10 {

				//для каждого символа для текущей биржи
				for i := range exchange.AllSymbols {

					//потокобезопасно получим мап с данными kline
					exchange.AllSymbols[i].AllKlineData.Lock()
					KlineDataMap := exchange.AllSymbols[i].AllKlineData.KlineDataMap
					exchange.AllSymbols[i].AllKlineData.Unlock()

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
								"1000",
								KlineDeleteDeltaTime,
							)
							//сделаем паузу между запросами
							time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
						}
					}
				}
			}
			//на каждой часовой минуте и 10 секунде
			if minutes == 0 && seconds == 10 {

				//для каждого символа для текущей биржи
				for i := range exchange.AllSymbols {

					//потокобезопасно получим мап с данными kline
					exchange.AllSymbols[i].AllKlineData.Lock()
					KlineDataMap := exchange.AllSymbols[i].AllKlineData.KlineDataMap
					exchange.AllSymbols[i].AllKlineData.Unlock()

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
									"1000",
									KlineDeleteDeltaTime,
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

// воркер для получения данных по опционам по таймеру
func GetOptionsDataTimer(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

	slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	//создадим тикер который будет срабатывать каждые 1 секунду
	ticker := time.NewTicker(1 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "exchangeName", exchange.ExchangeName)
	}()

	for {
		select {
		//если тикер дал тик
		case <-ticker.C:

			//получим текущее время
			now := time.Now().UTC()

			//отбросим все до секунды
			now = now.Truncate(time.Second)

			//получим секунды
			seconds := now.Second()

			// на каждой пятой секунде
			if seconds%5 == 0 {

				// slog.Warn("Сработал тикер", "func", "GetOptionsDataTimer", "exchangeName", exchange.ExchangeName)

				//для каждого символа - через индекс потому что внутри мьютексы
				for i := range exchange.AllSymbols {

					//если для символа стоит отметка получать категорию опционы
					if slices.Contains(exchange.AllSymbols[i].FetchFilter, "option") {
						optionsData, err := exchange.Client.GetOptions(exchange.AllSymbols[i].Symbol)
						if err != nil {
							slog.Error("Ошибка при получении данных по опционам", "exchangeName", exchange.ExchangeName, "symbol", exchange.AllSymbols[i].Symbol, "err", err)
						}

						//передадим данне по опционам в общую структуру
						exchange.AllSymbols[i].OptionsData = optionsData

					}
				}
			}

		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			slog.Info("Воркер остановлен", "exchangeName", exchange.ExchangeName)
			return
		}
	}
}

// ------- WEB SOCKET ВОРКЕРЫ ------

//------- ВОРКЕРЫ для работы с БД------

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

			//для каждого символа
			for j := range exchange.AllSymbols {
				//ключи для удаления
				var keysToDelete []string

				//блокируем мапу для чтения
				exchange.AllSymbols[j].AllKlineData.RLock()
				for k, klineMu := range exchange.AllSymbols[j].AllKlineData.KlineDataMap {
					klineMu.Lock()
					//проверим что текущее время больше записанного
					if now.After(klineMu.KlineData.DeleteTime) {
						//запишем ключ
						keysToDelete = append(keysToDelete, k)
					}
					klineMu.Unlock()
				}
				exchange.AllSymbols[j].AllKlineData.RUnlock()

				//если есть ключи для удаления
				if len(keysToDelete) > 0 {
					exchange.AllSymbols[j].AllKlineData.Lock()
					for _, k := range keysToDelete {
						delete(exchange.AllSymbols[j].AllKlineData.KlineDataMap, k)
						slog.Info("Запись kline удалена по истечению времени", "KlineDataMap[k]", exchange.AllSymbols[j].AllKlineData.KlineDataMap[k])
					}
					exchange.AllSymbols[j].AllKlineData.Unlock()
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
			//для каждого символа
			for i := range exchange.AllSymbols {

				var KlineDataMapKeys []string

				exchange.AllSymbols[i].AllKlineData.RLock()
				KlineDataMap := exchange.AllSymbols[i].AllKlineData.KlineDataMap
				for key := range KlineDataMap {
					KlineDataMapKeys = append(KlineDataMapKeys, key)
				}
				exchange.AllSymbols[i].AllKlineData.RUnlock()

				slog.Debug("Запрашиваемые Klines", "ExchangeName", exchange.ExchangeName, "Symbol", exchange.AllSymbols[i].Symbol, "KlineDataMapKeys", KlineDataMapKeys)
			}

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
