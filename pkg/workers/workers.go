package workers

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"time"

	"crypto_options_screener/pkg/shared"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

//------- РАЗОВЫЕ ФУНКЦИИ

// получает данные по всем монеткам для конкретной биржи и сохраняеет ее в БД
func GetAllSymbolsInfo(pool *pgxpool.Pool, exchange *shared.Exchange) {

	//для каждого символа для текущей биржи
	for symbolKey, symbolVal := range exchange.AllSymbols {

		//получим данные для текущей монетки
		symbolInfo, err := exchange.Client.GetSymbolInfo(symbolKey)
		if err != nil {
			slog.Error("Ошибка при получении", "exchangeName", exchange.ExchangeName, "symbol", symbolKey)
		}

		slog.Info("Получены данные SymbolInfo", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "symbolInfo", symbolInfo)

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
func GetKlineData(pool *pgxpool.Pool, exchange *shared.Exchange, symbol string, category string, interval string, limit string) {

	//получим данные
	klineData, err := exchange.Client.GetKline(symbol, category, interval, "1000")
	if err != nil {
		slog.Error("Ошибка при получении kline", "exchangeName", exchange.ExchangeName, "category", category, "symbol", symbol, "err", err)
		return
	}

	//если длина массива данных нулевая
	if len(klineData.KlineData) == 0 {
		slog.Warn("Длина данных len(klineData.KlineData) == 0", "exchangeName", exchange.ExchangeName, "category", category, "symbol", symbol)
		return
	}

	//в зависимости от категории
	switch category {
	case "spot":

		//запишем данные kline
		exchange.AllSymbols[symbol].KlineSpot[interval].SaveKlineData(klineData)

		// slog.Debug("Данные KlineSpot обнолены", "exchangeName", exchange.ExchangeName, "category", category, "symbol", symbol, "interval", interval, "data", exchange.AllSymbols[symbol].KlineSpot[interval])

	case "linear":

		//запишем данные kline
		exchange.AllSymbols[symbol].KlineLinear[interval].SaveKlineData(klineData)

		// slog.Debug("Данные KlineLinear обнолены", "exchangeName", exchange.ExchangeName, "category", category, "symbol", symbol, "interval", interval, "data", exchange.AllSymbols[symbol].KlineLinear[interval])
	}

}

// получает разово данные kline для всех монет и всех интервалов и сохранеет их в БД
func GetKlineDataInitial(pool *pgxpool.Pool, exchange *shared.Exchange, allKlineIntervals *[]string, category string) {

	//для каждого символа для текущей биржи
	for i := range exchange.AllSymbols {

		//если для символа есть отметка получении данных текущей категории
		if slices.Contains(exchange.AllSymbols[i].FetchFilter, category) {
			//для каждого интервала
			for _, interval := range *allKlineIntervals {

				//получим данные и сохраним их
				GetKlineData(pool, exchange, exchange.AllSymbols[i].Symbol, category, interval, "1000")

			}
		}

		//сделаем паузу между запросами
		time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
	}
}

//------- ВОРКЕРЫ И ТАЙМЕРЫ

// получает данные с определенным интервалом и записывает их в бд
func GetKlineDataTimer(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

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

					//для каждого интервала
					for _, interval := range []string{"15m"} {

						//если для символа стоит отметка получать данные spot
						if slices.Contains(exchange.AllSymbols[i].FetchFilter, "spot") {

							//получим и сохраним в общий объект и бд данные kline
							GetKlineData(pool, exchange, exchange.AllSymbols[i].Symbol, "spot", interval, "1000")
						}

						//если для символа стоит отметка получать данные linear
						if slices.Contains(exchange.AllSymbols[i].FetchFilter, "linear") {

							//получим и сохраним в общий объект и бд данные kline
							GetKlineData(pool, exchange, exchange.AllSymbols[i].Symbol, "linear", interval, "1000")
						}

					}

					//сделаем паузу между запросами
					time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
				}

			}
			//на каждой часовой минуте и 10 секунде
			if minutes == 0 && seconds == 10 {

				//для каждого символа для текущей биржи
				for i := range exchange.AllSymbols {

					//для каждого интервала
					for _, interval := range []string{"1h", "1D", "1W"} {

						//если для символа стоит отметка получать данные spot
						if slices.Contains(exchange.AllSymbols[i].FetchFilter, "spot") {

							//получим и сохраним в общий объект и бд данные kline
							GetKlineData(pool, exchange, exchange.AllSymbols[i].Symbol, "spot", interval, "1000")
						}

						//если для символа стоит отметка получать данные linear
						if slices.Contains(exchange.AllSymbols[i].FetchFilter, "linear") {

							//получим и сохраним в общий объект и бд данные kline
							GetKlineData(pool, exchange, exchange.AllSymbols[i].Symbol, "linear", interval, "1000")
						}

					}

					//сделаем паузу между запросами
					time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
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

// воркер для отправики сообщений ping
func WsPingTimer(ctx context.Context, conn *websocket.Conn, stopChan chan struct{}, exchangeName string) {

	slog.Info("Запуск воркера", "exchangeName", exchangeName)

	// Создаем тикер с интервалом в 20 секунд
	ticker := time.NewTicker(20 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "exchangeName", exchangeName)
	}()

	// Структура текстового запроса для Bybit / большинства бирж
	type JsonPing struct {
		Op string `json:"op"`
	}
	pingReq := JsonPing{Op: "ping"}

	for {
		select {
		case <-ticker.C:
			// Взводим дедлайн (таймаут) на 5 секунд. Если за это время сервер не пришлет нам
			// в обычном сообщении в ответ JSON {"op":"pong"}, сокет отключится и выдаст ошибку в чтении.
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			// Отправляем текстовый JSON Ping вместо системного фрейма
			if err := conn.WriteJSON(pingReq); err != nil {
				slog.Error("Ошибка отправки JSON Ping", "exchangeName", exchangeName, "error", err)
				conn.Close() // Принудительно закрываем сокет, чтобы разблокировать цикл чтения

				slog.Info("Работа вспомогательной горутины ping-pong завершена по отключению вебсокета", "exchangeName", exchangeName)
				return // Выходим из горутины, сокет отключен
			}

			slog.Info("На сервер отправлен запрос ping", "exchangeName", exchangeName)
		case <-stopChan:

			slog.Info("Воркер остановлен по сигналу", "exchangeName", exchangeName)
			return // Выходим по внешнемук сигналу

		case <-ctx.Done():
			slog.Info("Воркер остановлен по контксту", "exchangeName", exchangeName)
			return // Выходим по контексту приложения
		}
	}
}

// запускает воркера для получения данных с определенного канала
func WsWorker(ctx context.Context, exchange *shared.Exchange, streamType string, topicArr []string, dataChan chan []byte) {

	slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	//структура для подписки
	type Request struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}

	//установим подключение вебсокет с помощю клиента
	conn, _, err := exchange.Client.WsConnect(streamType)
	if err != nil {
		slog.Error("Ошибка подключения к ws", "exchangeName", exchange.ExchangeName, "streamType", streamType, "error", err)
	}
	defer conn.Close()

	//канал для остановки
	pingStopChan := make(chan struct{})

	//запустим воркер для пинга к серверу по интервалу
	go WsPingTimer(ctx, conn, pingStopChan, exchange.ExchangeName)

	//запрос подписки
	req := Request{
		Op:   "subscribe",
		Args: topicArr,
	}

	//подпишемся на каналы
	if err := conn.WriteJSON(req); err != nil {
		slog.Error("Ошибка подписки по ws", "exchangeName", exchange.ExchangeName, "error", err)
	}

	slog.Info("Оформлена подписка на Tickers по ws", "exchangeName", exchange.ExchangeName, "streamType", streamType, "topicArr", topicArr)

	//горутина для отключения воркера по контексту
	go func() {

		//блокируем горутину до события выхода
		<-ctx.Done()

		// закрываем соедиение
		conn.Close()

		slog.Info("Работа горутины закрывающей соеденение ws завершена", "exchangeName", exchange.ExchangeName, "streamType", streamType)
	}()

	//цикл чтения данных из вебсокета
	for {

		//если контекст отменен — выходим, не пытаясь подключиться снова
		if ctx.Err() != nil {
			slog.Info("Ws linear отключен по контексту", "exchangeName", exchange.ExchangeName, "streamType", streamType, "topicArr", topicArr)
			break
		}

		//читаем сообщение
		_, message, err := conn.ReadMessage()
		if err != nil {
			// Если контекст отменен — это штатное закрытие сокета, выходим без паники
			if ctx.Err() != nil {
				slog.Info("Websocket отключен по контексту", "exchangeName", exchange.ExchangeName, "streamType", streamType, "topicArr", topicArr)
				break
			}

			slog.Error("Ошибка чтения ws", "exchangeName", exchange.ExchangeName, "error", err)

			//остановим горутину пинга на сервер
			close(pingStopChan)

			slog.Info("Запущен таймер переподключение WebSocket", "exchangeName", exchange.ExchangeName, "streamType", streamType)

			//таймер для переподключения
			ticker := time.NewTicker(20 * time.Second)

			//метка для остановки for изнутри select
		ReconnectLoop:
			//цикл переподключения
			for {
				select {
				//если тикер дал тик
				case <-ticker.C:

					slog.Info("Переподключение WebSocket...", "exchangeName", exchange.ExchangeName)

					//переопределим подключение
					conn, _, err = exchange.Client.WsConnect(streamType)
					if err != nil {
						slog.Error("Ошибка подключения к ws", "exchangeName", exchange.ExchangeName, "error", err)

						//продолжим цикл for - да да continue продолжает for а если break то прерывает селект)))
						continue
					}
					//если переподключение прошло успешно

					//переопределим канал для остановки пинг воркера
					pingStopChan = make(chan struct{})

					//запустим воркер для пинга к серверу по интервалу на новом соединении и сновым каналом управления
					go WsPingTimer(ctx, conn, pingStopChan, exchange.ExchangeName)

					//подпишемся на каналы
					if err := conn.WriteJSON(req); err != nil {
						slog.Error("Ошибка подписки по ws", "exchangeName", exchange.ExchangeName, "error", err)
					}

					slog.Info("Оформлена подписка на Tickers по ws", "exchangeName", exchange.ExchangeName, "streamType", streamType, "topicArr", topicArr)

					//отановим таймер подключения
					ticker.Stop()
					slog.Debug("Таймер для переподключения к WebsocketLinear остановлен", "exchangeName", exchange.ExchangeName)

					//остановим цикл for через метку ReconnectLoop
					break ReconnectLoop

				//если работа программы остановлены
				case <-ctx.Done():
					// Завершаем работу по контексту
					slog.Info("Воркер остановлен", "exchangeName", exchange.ExchangeName)
					return
				}
			}
		}

		// slog.Debug("Получено сообщение от сервера", "string(message)", string(message))

		//еcли ответ не нулевой - такое бывает при переподключении
		if string(message) != "" {

			isPong, err := exchange.Client.WsDecodePong(message)
			if err != nil {
				slog.Error("Ошибка при декодировании pong", "exchangeName", exchange.ExchangeName, "error", err)
				slog.Error("Полное сообщение от сервера", "string(message)", string(message))
			}
			//если пришло сообщение pong (в ответ на наш ping)
			if isPong == true {

				slog.Info("От сервера получен ответ pong", "exchangeName", exchange.ExchangeName)

				//отключим дедлайн
				conn.SetReadDeadline(time.Time{})

				//ВАЖНО!!! пропускаем остаток цикла
				continue
			}

			//если это какоето другое сообщение
			if isPong == false {
				//отправим сообщение на внешний канал
				dataChan <- message
			}
		}
	}
}

// работает с вебсокетом в котором получает данные для формирования последней свечки и сохраняет эти данные
func GetKlineCandleLast(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange, category string) {

	//тип потока
	var streamType string

	//список тем для подписки
	var topicArr []string

	//для каждого символа для текущей биржи
	for i := range exchange.AllSymbols {

		//если для символа существует запиись получать данные из соответствующей категории
		if category == "spot" && slices.Contains(exchange.AllSymbols[i].FetchFilter, "spot") {

			//определим тип потока
			streamType = "ws-spot-channel"

			//получим сформированный топик от клиента
			topic, err := exchange.Client.FormTopic(streamType, []string{exchange.AllSymbols[i].Symbol})
			if err != nil {
				slog.Error("Ошибка при формировании топика", "exchangeName", exchange.ExchangeName, "symbol", exchange.AllSymbols[i].Symbol, "error", err)
			}
			//добавим элемент
			topicArr = append(topicArr, topic)
		}

		//если входящая категория linear и для символа стоит отметка получать данные linear
		if category == "linear" && slices.Contains(exchange.AllSymbols[i].FetchFilter, "linear") {

			//определим тип потока
			streamType = "ws-linear-channel"

			//получим сформированный топик от клиента
			topic, err := exchange.Client.FormTopic(streamType, []string{exchange.AllSymbols[i].Symbol})
			if err != nil {
				slog.Error("Ошибка при формировании топика", "exchangeName", exchange.ExchangeName, "symbol", exchange.AllSymbols[i].Symbol, "error", err)
			}
			//добавим элемент
			topicArr = append(topicArr, topic)
		}
	}

	//канал для получения данных
	dataChan := make(chan []byte, 100)

	//запустим горутину для обработки данных
	go WsWorker(ctx, exchange, streamType, topicArr, dataChan)

	// обработка данных полученная от ws воркера
	for {
		select {
		case message := <-dataChan:

			// slog.Debug("Получено сообщение от ws воркера", "string(message)", string(message))

			//декодируем сообщение с помощью клиента
			data, err := exchange.Client.WsDecodeLinearLastPrice(message)
			if err != nil {
				slog.Error("Ошибка при декодировании lastPrice", "exchangeName", exchange.ExchangeName, "error", err)
			}

			//если есть изменения по LastPrice
			if data.LastPrice != "" {

				// slog.Debug("Получены данные lastPrice", "exchangeName", exchange.ExchangeName, "symbol", data.Symbol, "LastPrice", data.LastPrice)

				//получим последнюю цену по символу
				for symbolKey, symbolVal := range exchange.AllSymbols {

					//если это данные по текущему символу
					if symbolKey+"USDT" == data.Symbol {

						if streamType == "ws-spot-channel" {

							//для каждого интервала данных kline
							for _, intervalVal := range symbolVal.KlineSpot {
								//обновим последнюю свечку
								intervalVal.UpdateKlineLastCandle(data.LastPrice)
							}

						}
						if streamType == "ws-linear-channel" {

							//для каждого интервала данных kline
							for _, intervalVal := range symbolVal.KlineLinear {
								//обновим последнюю свечку
								intervalVal.UpdateKlineLastCandle(data.LastPrice)
							}

							// slog.Warn("Обновленная lastCandle",  "exchangeName", exchange.ExchangeName, "symbol", data.Symbol, "streamType", streamType, "lastCandle", exchange.AllSymbols[i].LastCandleLinear.GetKlineLastCandle())

						}

						break
					}
				}
			}

		case <-ctx.Done():
			slog.Info("Внешний обработчик остановлен по контексту", "exchangeName", exchange.ExchangeName)
			return
		}
	}

}

// воркер для сохранения данных в бд по таймеру
func SaveSymbolDataToDbTimer(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

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

			//получим минуты
			minutes := now.Minute()

			//если мы прошли отметку кратную 15 мин
			if minutes%15 == 0 && seconds == 10 {

				//для каждого символа - через индекс потому что внутри мьютексы
				for symbolKey, symbolVal := range exchange.AllSymbols {

					//сдвигаем время на 15 минут назад
					fifteenMinutesAgo := now.Add(-15 * time.Minute)

					//получаем миллисекунды (тип int64)
					timeMs := fifteenMinutesAgo.UnixMilli()

					//последняя цена спота
					lastPriceSpot := "0"
					if symbolVal.KlineSpot["15m"] != nil {
						lastPriceSpot = symbolVal.KlineSpot["15m"].GetKlineLastCandleClosePrice()
					}
					//последняя цена фьючерса
					lastPriceLinear := "0"
					if symbolVal.KlineLinear["15m"] != nil {
						lastPriceLinear = symbolVal.KlineLinear["15m"].GetKlineLastCandleClosePrice()
					}

					//сформируем объект для записи
					symbolData := shared.SymbolData{
						ExchangeName:    exchange.ExchangeName,
						Symbol:          symbolVal.Symbol,
						LastPriceSpot:   lastPriceSpot,
						LastPriceLinear: lastPriceLinear,
						LinearData:      []shared.LinearData{},
						OptionsData:     symbolVal.OptionsData,
						TimeMs:          strconv.FormatInt(timeMs, 10),
					}

					// 4. Обязательно переводим структуру Go в JSON-строку перед отправкой в БД
					jsonData, err := json.Marshal(symbolData)
					if err != nil {
						slog.Error("Ошибка при создании json из symbolData", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "err", err)
						return
					}

					// SQL-запрос для PostgreSQL (плейсхолдеры \$1, \$2...)
					query := `
						INSERT INTO all_symbol_data_new (exchange_name, symbol, symbol_data, time_ms)
						VALUES ($1, $2, $3, $4);
					`

					// Создаем таймаут НА ОСНОВЕ переданного ctx. Если сработает сигнал остановки приложения ИЛИ пройдет 3 секунды — контекст закроется и pool выдаст ошибку
					dbCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					defer cancel()

					// Выполняем запрос через пул соединений
					_, err = pool.Exec(dbCtx, query, exchange.ExchangeName, symbolKey, string(jsonData), timeMs)
					if err != nil {
						slog.Error("Ошибка при сохранении SymbolData", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "timeMs", timeMs, "err", err)
						return
					}

					slog.Info("Данные SymbolData сохранены в бд", "exchangeName", exchange.ExchangeName, "symbol", symbolKey, "timeMs", timeMs)
				}
			}

		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			slog.Info("Воркер остановлен", "exchangeName", exchange.ExchangeName)
			return
		}
	}
}

// воркер раз в 10 сек показывает размер объекта
func GetMemStatsTimer(ctx context.Context, exchange *shared.Exchange) {

	slog.Info("Запуск воркера", "exchangeName", exchange.ExchangeName)

	//создадим тикер который каждые 10 секунд будет срабатывать
	ticker := time.NewTicker(60 * time.Second)
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
