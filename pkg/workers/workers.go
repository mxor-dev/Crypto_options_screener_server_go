package workers

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"slices"
	"time"

	"crypto_options_screener/pkg/shared"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

//------- СОХРАНЕНИЕ ДАННЫХ БД

// сохраняет данные kline в бд
func SaveKlineToDb(pool *pgxpool.Pool, klineData *shared.KlineData) {

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
func GetAllSymbolsInfo(pool *pgxpool.Pool, exchange *shared.Exchange) {

	//для каждого символа для текущей биржи
	for i := range exchange.Symbols {

		//получим данные для текущей монетки
		symbolInfo, err := exchange.Client.GetSymbolInfo(exchange.Symbols[i].Symbol)
		if err != nil {
			slog.Error("Ошибка при получении", "func", "GetAllSymbolsInfo", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol)
		}

		slog.Info("Получены данные SymbolInfo", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "symbolInfo", symbolInfo)

		//запишем данные в общую структуру
		exchange.Symbols[i].Info.SaveInfo(symbolInfo)

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
		// 	slog.Error("Ошибка при сохранении данных в БД", "func", "GetAllSymbolsInfo", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol)
		// }

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}

// получает разово данные kline для всех монет и всех интервалов и сохранеет их в БД
func GetKlineDataInitial(pool *pgxpool.Pool, exchange *shared.Exchange, allKlineIntervals *[]string, category string) {

	//для каждого символа для текущей биржи
	for i := range exchange.Symbols {

		//если для символа есть отметка получении данных текущей категории
		if slices.Contains(exchange.Symbols[i].Categories, category) {
			//для каждого интервала
			for _, interval := range *allKlineIntervals {

				//получим данные
				klineData, err := exchange.Client.GetKline(exchange.Symbols[i].Symbol, category, interval, "1000")
				if err != nil {
					slog.Error("Ошибка при получении kline spot", "fun", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "err", err)
				}

				//в зависимости от категории
				switch category {
				case "spot":

					//запишем данные kline
					exchange.Symbols[i].KlineSpot.SaveKlineData(klineData)

					//запишем последнюю свечку
					exchange.Symbols[i].LastCandleSpot.SaveKlineLastCandle(klineData.KlineData[len(klineData.KlineData)-1])

				case "linear":
					//запишем данные kline
					exchange.Symbols[i].KlineLinear.SaveKlineData(klineData)

					//запишем последнюю свечку
					exchange.Symbols[i].LastCandleLinear.SaveKlineLastCandle(klineData.KlineData[len(klineData.KlineData)-1])

				}

				//сохранение данных в бд
				SaveKlineToDb(pool, &klineData)

			}
		}

		//сделаем паузу между запросами
		time.Sleep(exchange.Intervals.RestDataGetInterval)
	}
}

// получает данные с определенным интервалом и записывает их в бд
func GetKlineDataTimer(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

	slog.Info("Запуск воркера", "func", "GetKlineDataTimer", "exchangeName", exchange.Name)

	//создадим тикер который каждые 1 секунду будет срабатывать
	ticker := time.NewTicker(1 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "func", "GetKlineDataTimer", "exchangeName", exchange.Name)
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
				for i := range exchange.Symbols {

					//для каждого интервала
					for _, interval := range []string{"15m"} {

						//если для символа стоит отметка получать данные spot
						if slices.Contains(exchange.Symbols[i].Categories, "spot") {

							//получим данные
							klineData, err := exchange.Client.GetKline(exchange.Symbols[i].Symbol, "spot", interval, "1000")
							if err != nil {
								slog.Error("Ошибка при получении kline spot", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//запишем данные в общую структуру биржи
							exchange.Symbols[i].KlineSpot.SaveKlineData(klineData)

							//запишем последнюю свечку
							exchange.Symbols[i].LastCandleSpot.SaveKlineLastCandle(klineData.KlineData[len(klineData.KlineData)-1])

							//сохранение данных в бд
							SaveKlineToDb(pool, &klineData)
						}

						if slices.Contains(exchange.Symbols[i].Categories, "linear") {

							//получим данные
							klineData, err := exchange.Client.GetKline(exchange.Symbols[i].Symbol, "linear", interval, "1000")
							if err != nil {
								slog.Error("Ошибка при получении kline linear", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//запишем данные в общую структуру биржи
							exchange.Symbols[i].KlineLinear.SaveKlineData(klineData)

							//запишем последнюю свечку
							exchange.Symbols[i].LastCandleLinear.SaveKlineLastCandle(klineData.KlineData[len(klineData.KlineData)-1])

							//сохранение данных в бд
							SaveKlineToDb(pool, &klineData)
						}

					}

					//сделаем паузу между запросами
					time.Sleep(exchange.Intervals.RestDataGetInterval)
				}

			}
			//на каждой часовой минуте и 10 секунде
			if minutes == 0 && seconds == 10 {

				//для каждого символа для текущей биржи
				for i := range exchange.Symbols {

					//для каждого интервала
					for _, interval := range []string{"1h", "1D", "1W"} {

						//если для символа стоит отметка получать данные spot
						if slices.Contains(exchange.Symbols[i].Categories, "spot") {

							//получим данные
							klineData, err := exchange.Client.GetKline(exchange.Symbols[i].Symbol, "spot", interval, "1000")

							if err != nil {
								slog.Error("Ошибка при получении kline spot", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//запишем данные в общую структуру биржи
							exchange.Symbols[i].KlineSpot.SaveKlineData(klineData)

							//запишем последнюю свечку
							exchange.Symbols[i].LastCandleSpot.SaveKlineLastCandle(klineData.KlineData[len(klineData.KlineData)-1])

							//сохранение данных в бд
							SaveKlineToDb(pool, &klineData)
						}

						if slices.Contains(exchange.Symbols[i].Categories, "linear") {

							//получим данные
							klineData, err := exchange.Client.GetKline(exchange.Symbols[i].Symbol, "linear", interval, "1000")
							if err != nil {
								slog.Error("Ошибка при получении kline linear", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "err", err)
							}

							slog.Info("Получены данные kline", "func", "GetKlineDataInitial", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "len(klineData.KlineData)", len(klineData.KlineData))

							//запишем данные в общую структуру биржи
							exchange.Symbols[i].KlineLinear.SaveKlineData(klineData)

							//запишем последнюю свечку
							exchange.Symbols[i].LastCandleLinear.SaveKlineLastCandle(klineData.KlineData[len(klineData.KlineData)-1])

							//сохранение данных в бд
							SaveKlineToDb(pool, &klineData)
						}

					}

					//сделаем паузу между запросами
					time.Sleep(exchange.Intervals.RestDataGetInterval)
				}

			}

		//если работа программы остановлены
		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			return
		}
	}
}

// запускает воркера для получения данных с определенного канала
func WsWorker(ctx context.Context, exchange *shared.Exchange, streamType string, topicArr []string, dataChan chan []byte) {

	//структура для подписки
	type Request struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}

	//внутренняя функция - запускает фоновый тикер для отправки текстовых JSON-пингов
	startJsonPingWorker := func(conn *websocket.Conn, stopChan chan struct{}) {

		// Создаем тикер с интервалом в 20 секунд
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

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
					slog.Error("Ошибка отправки JSON Ping", "func", "startJsonPingWorker", "exchangeName", exchange.Name, "error", err)
					conn.Close() // Принудительно закрываем сокет, чтобы разблокировать цикл чтения

					slog.Info("Работа вспомогательной горутины ping-pong завершена по отключению вебсокета", "func", "startJsonPingWorker", "exchangeName", exchange.Name)
					return // Выходим из горутины, сокет отключен
				}

				slog.Debug("На сервер отправлен запрос ping", "func", "startJsonPingWorker", "exchangeName", exchange.Name)
			case <-stopChan:

				slog.Info("Работа вспомогательной горутины ping-pong остановлена сигналом снаружи", "func", "startJsonPingWorker", "exchangeName", exchange.Name)
				return // Выходим по внешнемук сигналу

			case <-ctx.Done():
				slog.Info("Работа вспомогательной горутины ping-pong завершена по контексту", "func", "startJsonPingWorker", "exchangeName", exchange.Name)
				return // Выходим по контексту приложения
			}
		}
	}

	//установим подключение вебсокет с помощю клиента
	conn, _, err := exchange.Client.WsConnect(streamType)
	if err != nil {
		slog.Error("Ошибка подключения к ws", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "streamType", streamType, "error", err)
	}
	defer conn.Close()

	//канал для остановки
	pingStopChan := make(chan struct{})

	//запустим воркер для пинга к серверу по интервалу
	go startJsonPingWorker(conn, pingStopChan)

	//запрос подписки
	req := Request{
		Op:   "subscribe",
		Args: topicArr,
	}

	//подпишемся на каналы
	if err := conn.WriteJSON(req); err != nil {
		slog.Error("Ошибка подписки по ws", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "error", err)
	}

	slog.Info("Оформлена подписка на Tickers по ws", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "streamType", streamType, "topicArr", topicArr)

	//горутина для отключения воркера по контексту
	go func() {

		//блокируем горутину до события выхода
		<-ctx.Done()

		// закрываем соедиение
		conn.Close()

		slog.Info("Работа горутины закрывающей соеденение ws завершена", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "streamType", streamType)
	}()

	//цикл чтения данных из вебсокета
	for {

		//если контекст отменен — выходим, не пытаясь подключиться снова
		if ctx.Err() != nil {
			slog.Info("Ws linear отключен по контексту", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "streamType", streamType, "topicArr", topicArr)
			break
		}

		//читаем сообщение
		_, message, err := conn.ReadMessage()
		if err != nil {
			// Если контекст отменен — это штатное закрытие сокета, выходим без паники
			if ctx.Err() != nil {
				slog.Info("Websocket отключен по контексту", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "streamType", streamType, "topicArr", topicArr)
				break
			}

			slog.Error("Ошибка чтения ws", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "error", err)

			//остановим горутину пинга на сервер
			close(pingStopChan)

			slog.Info("Запущен таймер переподключение WebSocket", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "streamType", streamType)

			//таймер для переподключения
			ticker := time.NewTicker(20 * time.Second)

			//метка для остановки for изнутри select
		ReconnectLoop:
			//цикл переподключения
			for {
				select {
				//если тикер дал тик
				case <-ticker.C:

					slog.Info("Переподключение WebSocket...", "func", "GetKlineCandleLast", "exchangeName", exchange.Name)

					//переопределим подключение
					conn, _, err = exchange.Client.WsConnect(streamType)
					if err != nil {
						slog.Error("Ошибка подключения к ws", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "error", err)

						//продолжим цикл for - да да continue продолжает for а если break то прерывает селект)))
						continue
					}
					//если переподключение прошло успешно

					//переопределим канал для остановки пинг воркера
					pingStopChan = make(chan struct{})

					//запустим воркер для пинга к серверу по интервалу на новом соединении и сновым каналом управления
					go startJsonPingWorker(conn, pingStopChan)

					//подпишемся на каналы
					if err := conn.WriteJSON(req); err != nil {
						slog.Error("Ошибка подписки по ws", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "error", err)
					}

					slog.Info("Оформлена подписка на Tickers по ws", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "streamType", streamType, "topicArr", topicArr)

					//отановим таймер подключения
					ticker.Stop()
					slog.Debug("Таймер для переподключения к WebsocketLinear остановлен", "func", "GetKlineCandleLast", "exchangeName", exchange.Name)

					//остановим цикл for через метку ReconnectLoop
					break ReconnectLoop

				//если работа программы остановлены
				case <-ctx.Done():
					// Завершаем работу по контексту
					return
				}
			}
		}

		// slog.Debug("Получено сообщение от сервера", "string(message)", string(message))

		//еcли ответ не нулевой - такое бывает при переподключении
		if string(message) != "" {

			isPong, err := exchange.Client.WsDecodePong(message)
			if err != nil {
				slog.Error("Ошибка при декодировании pong", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "error", err)
				slog.Error("Полное сообщение от сервера", "string(message)", string(message))
			}
			//если пришло сообщение pong (в ответ на наш ping)
			if isPong == true {

				slog.Debug("От сервера получен ответ pong", "func", "GetKlineCandleLast", "exchangeName", exchange.Name)

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
	for i := range exchange.Symbols {

		//если для символа существует запиись получать данные из соответствующей категории
		if category == "spot" && slices.Contains(exchange.Symbols[i].Categories, "spot") {

			//определим тип потока
			streamType = "ws-spot-channel"

			//получим сформированный топик от клиента
			topic, err := exchange.Client.FormTopic(streamType, []string{exchange.Symbols[i].Symbol})
			if err != nil {
				slog.Error("Ошибка при формировании топика", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "error", err)
			}
			//добавим элемент
			topicArr = append(topicArr, topic)
		}

		//если входящая категория linear и для символа стоит отметка получать данные linear
		if category == "linear" && slices.Contains(exchange.Symbols[i].Categories, "linear") {

			//определим тип потока
			streamType = "ws-linear-channel"

			//получим сформированный топик от клиента
			topic, err := exchange.Client.FormTopic(streamType, []string{exchange.Symbols[i].Symbol})
			if err != nil {
				slog.Error("Ошибка при формировании топика", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "error", err)
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

			slog.Debug("Получено сообщение от ws воркера", "string(message)", string(message))

			//декодируем сообщение с помощью клиента
			data, err := exchange.Client.WsDecodeLinearLastPrice(message)
			if err != nil {
				slog.Error("Ошибка при декодировании lastPrice", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "error", err)
			}

			//если есть изменения по LastPrice
			if data.LastPrice != "" {

				// slog.Debug("Получены данные lastPrice", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "symbol", data.Symbol, "LastPrice", data.LastPrice)

				//получим последнюю цену по символу
				for i := range exchange.Symbols {

					//если это данные по текущему символу
					if exchange.Symbols[i].Symbol+"USDT" == data.Symbol {

						if streamType == "ws-spot-channel" {

							//обновим свечку
							exchange.Symbols[i].LastCandleSpot.UpdateKlineLastCandle(data.LastPrice)
						}
						if streamType == "ws-linear-channel" {

							//обновим свечку
							exchange.Symbols[i].LastCandleLinear.UpdateKlineLastCandle(data.LastPrice)

							// slog.Warn("Обновленная lastCandle", "func", "GetKlineCandleLast", "exchangeName", exchange.Name, "symbol", data.Symbol, "streamType", streamType, "lastCandle", exchange.Symbols[i].LastCandleLinear.GetKlineLastCandle())

						}

						break
					}
				}
			}

		case <-ctx.Done():
			slog.Info("Внешний обработчик остановлен по контексту", "func", "GetKlineCandleLast", "exchangeName", exchange.Name)
			return
		}
	}

}

// воркер для получения данных по опционам по таймеру
func GetOptionsDataTimer(ctx context.Context, pool *pgxpool.Pool, exchange *shared.Exchange) {

	slog.Info("Запуск воркера", "func", "GetOptionsDataTimer", "exchangeName", exchange.Name)

	//создадим тикер который каждые 5 секунд будет срабатывать
	ticker := time.NewTicker(5 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "func", "GetOptionsDataTimer", "exchangeName", exchange.Name)
	}()

	for {
		select {
		//если тикер дал тик
		case <-ticker.C:

			// slog.Warn("Сработал тикер", "func", "GetOptionsDataTimer", "exchangeName", exchange.Name)

			//для каждого символа - через индекс потому что внутри мьютексы
			for i := range exchange.Symbols {

				//если для символа стоит отметка получать категорию опционы
				if slices.Contains(exchange.Symbols[i].Categories, "option") {
					optionsData, err := exchange.Client.GetOptions(exchange.Symbols[i].Symbol)
					if err != nil {
						slog.Error("Ошибка при получении данных по опционам", "func", "GetOptionsDataTimer", "exchangeName", exchange.Name, "symbol", exchange.Symbols[i].Symbol, "err", err)
					}

					//передадим данне по опционам в общую структуру
					exchange.Symbols[i].OptionsData = optionsData
				}
			}

		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			return
		}
	}
}

// воркер раз в 10 сек показывает размер объекта
func GetSizeTimer(ctx context.Context, exchange *shared.Exchange) {

	slog.Info("Запуск воркера", "func", "GetOptionsDataTimer", "exchangeName", exchange.Name)

	//создадим тикер который каждые 10 секунд будет срабатывать
	ticker := time.NewTicker(60 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Таймер остановлен", "func", "GetSizeTimer", "exchangeName", exchange.Name)
	}()

	for {
		select {
		//если тикер дал тик
		case <-ticker.C:

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			slog.Warn("Метрики памяти", "func", "GetSizeTimer", "Alloc_MB", float64(m.Alloc)/1024/1024, "Sys_MB", float64(m.Sys)/1024/1024, "HeapObjects", m.HeapObjects)

		case <-ctx.Done():
			// Завершаем работу функции по контексту - все defer отрабатают автоматически
			return
		}
	}
}
