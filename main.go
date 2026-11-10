package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"crypto_options_screener/pkg/exchanges/bybit"
	"crypto_options_screener/pkg/shared"
	"crypto_options_screener/pkg/workers"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
)

func main() {

	//------- ЛОГГЕР

	// Обработчик текста логов
	handler := tint.NewTextHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelDebug, // Уровень логирования
		TimeFormat: time.Kitchen,    // Короткое время (например, 15:04)
		AddSource:  true,            // Имя файла модуля и номер строки!

		//фоорматтер для вывода атрибутов - выводим "имя_файла:номер_строки имя_функции"
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Проверяем, что обрабатываем системное поле источника (source)
			if a.Key == slog.SourceKey {
				if source, ok := a.Value.Any().(*slog.Source); ok {
					// Получаем только чистое имя функции без пути к пакету
					funcName := source.Function
					if idx := strings.LastIndex(funcName, "."); idx != -1 {
						funcName = funcName[idx+1:]
					}
					// Форматируем вывод в виде: [функция -> файл:строка]
					a.Value = slog.StringValue(fmt.Sprintf("%s:%d %s", filepath.Base(source.File), source.Line, funcName))
				}
			}
			return a
		},
	})

	//логгер
	logger := slog.New(handler)

	//установим логгер по умолчанию
	slog.SetDefault(logger)

	slog.Info("Старт приложения")

	//------- ГЛАВНЫЙ КОНТЕКСТ

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	//------- ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ

	//загрузим переменные окружения
	if err := godotenv.Load(); err != nil {
		slog.Error("Ошибка при загрузке файла .env")
	}

	//------- БАЗА ДАННЫХ

	//сформируем url для подключения к бд - поскольку в пароле могут быть @ и другие символы, формировать нужно именно так
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD")),
		Host:   fmt.Sprintf("%s:%s", os.Getenv("DB_HOST"), os.Getenv("DB_PORT")),
		Path:   os.Getenv("DB_NAME"),
	}

	//сформируем строку для подключения к БД
	dbUrl := u.String()

	pool, err := pgxpool.New(context.Background(), dbUrl)
	if err != nil {
		slog.Error("Не удалось создать пул соединений для базы данных", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	//тест соединение с бд
	err = pool.Ping(context.Background())
	if err != nil {
		slog.Error("Не удалось подключиться к базе данных", "error", err)
		os.Exit(1)
	}

	slog.Info("Соединение с базой данных установлено.")

	//------ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ

	//настройки для всех бирж в виде хеш-таблицы
	allExchanges := map[string]*shared.Exchange{
		"Bybit": {
			IsActive:     true,
			ExchangeName: "Bybit",
			AllSymbols: map[string]*shared.SymbolData{
				"BTC":  {Symbol: "BTC", FetchFilter: []string{"spot", "linear", "option"}},
				"DOGE": {Symbol: "DOGE", FetchFilter: []string{"spot", "linear", "option"}},
				"ETH":  {Symbol: "ETH", FetchFilter: []string{"spot", "linear", "option"}},
				// "HYPE": {Symbol: "HYPE", FetchFilter: []string{"linear", "option"}},
				// "MNT":  {Symbol: "MNT", FetchFilter: []string{"linear", "option"}},
				// "MU":   {Symbol: "MU", FetchFilter: []string{"linear", "option"}},
				"NVDA": {Symbol: "NVDA", FetchFilter: []string{"linear", "option"}}, //*
				// "QQQ":  {Symbol: "QQQ", FetchFilter: []string{"linear", "option"}},
				// "SKHY": {Symbol: "SKHY", FetchFilter: []string{"linear", "option"}},
				// "SNDK": {Symbol: "SNDK", FetchFilter: []string{"linear", "option"}},
				// "SOL":  {Symbol: "SOL", FetchFilter: []string{"linear", "option"}},
				// "SOXL": {Symbol: "SOXL", FetchFilter: []string{"linear", "option"}},
				"SPCX": {Symbol: "SPCX", FetchFilter: []string{"linear", "option"}}, //*
				// "TSLA": {Symbol: "TSLA", FetchFilter: []string{"linear", "option"}},
				// "XAUT": {Symbol: "XAUT", FetchFilter: []string{"linear", "option"}},
				// "XRP":  {Symbol: "XRP", FetchFilter: []string{"linear", "option"}},
			},
			FetchIntervals: shared.FetchIntervals{
				RestDataGetInterval:    50 * time.Millisecond,
				TickersDataGetInterval: 10 * time.Second,

				// KlineDeleteDeltaTime : 30 * time.Second,
				// KlineDeleteDeltaTime: 1 * time.Minute,
				KlineDeleteDeltaTime: 1 * time.Hour,

				WsDeadLineDelta:     30 * time.Second,
				WsCmdMsgDeleteDelta: 30 * time.Second, //дальше уже вебсокет отключается
			},
			KlineLimit: "1000",
			FeeRates: shared.FeeRates{
				Spot: shared.FeeRatesSpot{
					MakerFeeRate: 0.1,
					TakerFeeRate: 0.31,
				},
				Futures: shared.FeeRatesFutures{
					MakerFeeRate: 0.02,
					TakerFeeRate: 0.055,
				},
				Options: shared.FeeRatesOptions{
					MaxPartRate:        7,
					LiquidationFeeRate: 0.2,
					MakerFeeRate:       0.02,
					TakerFeeRate:       0.03,
					ExpirationFeeRate:  0.015,
				},
			},
		},
		"Mexc": {
			IsActive:     true,
			ExchangeName: "Mexc",
			AllSymbols:   map[string]*shared.SymbolData{},
			FetchIntervals: shared.FetchIntervals{
				RestDataGetInterval:    200 * time.Millisecond,
				TickersDataGetInterval: 10 * time.Second,
			},
			KlineLimit: "1000",
		},
	}

	//интервалы kline
	// allKlineIntervals := []string{
	// 	"15m",
	// 	"1h",
	// 	"1D",
	// 	"1W",
	// }

	// для каждой биржи - ЗАПОЛНИМ НАЧАЛЬНЫЕ ДАННЫЕ СТРУКТУР
	for exchangeName := range allExchanges {

		// Перебираем символы внутри конкретной биржи по ключу (symbolName)
		for symbolName := range allExchanges[exchangeName].AllSymbols {

			// Получаем ПРЯМОЙ указатель на объект в мапе, чтобы менять оригинал
			symbolData := allExchanges[exchangeName].AllSymbols[symbolName]

			// Инициализируем пустую структуру для тикеров
			symbolData.TikersData = shared.TikersDataMu{}
		}

		// Инициализируем мапу клайнов (Мьютексы на старте лочить НЕ НУЖНО)
		allExchanges[exchangeName].AllKlineData = shared.KlineDataMapMu{
			KlineDataMap: make(map[string]*shared.KlineDataMu),
		}

		allExchanges[exchangeName].Ws = map[string]*shared.WsData{}

		//счетчик ID
		allExchanges[exchangeName].GenID = &shared.GenIDMu{}

	}

	//****** ПОЛУЧЕНИЕ И ОБРАБОТКА ДАННЫХ СО ВСЕХ БИРЖ *********************************************************************

	//для каждой биржи
	for exKey := range allExchanges {

		slog.Info("Состояние биржи", "exchangeName", allExchanges[exKey].ExchangeName, "IsActive", allExchanges[exKey].IsActive)

		//если биржа активна
		if allExchanges[exKey].IsActive {

			//выберем имя биржи
			switch exKey {
			case "Bybit":
				//создадим клиента
				allExchanges[exKey].Client = bybit.New("public_data", "public_data")

				slog.Info("Создан клиент для биржи", "exchangeName", exKey)
			case "Mexc":
				slog.Info("Создание клиента для биржи будет реализовано позднее", "exchangeName", exKey)
			default:
				slog.Warn("Неизвестная биржа", "exchangeName", exKey)
			}

			//если клинет для биржи создан
			if allExchanges[exKey].Client != nil {
				//запускаем воркеров

				//получим информацию для всех монет на бирже
				workers.GetAllSymbolsInfo(pool, allExchanges[exKey])

				//запускаем горутину воркера для получения обновления данных kline по таймеру
				go workers.GetDataSequentalTimer(ctx, allExchanges[exKey])

				//в зависимости от биржи для получнеия последней свечи могут использоваться один или два ws воркера
				switch exKey {
				case "Bybit":
					//запускаем ws воркера для получения данных по spot
					err = workers.StartWebSocket(ctx, allExchanges[exKey], "spot", allExchanges[exKey].Client.GetBaseEndpointSpot())
					if err != nil {
						slog.Error("Ошибка при старте ws worker", "exchangeName", allExchanges[exKey], "baseEndPoint", allExchanges[exKey].Client.GetBaseEndpointSpot())
					}

					//запускаем ws воркера для получения данных по linear
					err = workers.StartWebSocket(ctx, allExchanges[exKey], "linear", allExchanges[exKey].Client.GetBaseEndpointLinear())
					if err != nil {
						slog.Error("Ошибка при старте ws worker", "exchangeName", allExchanges[exKey], "baseEndPoint", allExchanges[exKey].Client.GetBaseEndpointLinear())
					}

				case "Mexc":
					slog.Info("Запуск ws worker для биржи будет реализовано позднее", "exchangeName", exKey)
				}

				//запускаем воркера для получения данных по опционам по таймеру
				// go workers.GetOptionsDataTimer(ctx, pool, exchangeVal)

				//запуск воркера для сохранения данных
				// go workers.SaveSymbolDataToDbTimer(ctx, pool, exchangeVal)

				//запуск воркера для очистки klineData
				go workers.ExpiredDataDeleteTimer(ctx, allExchanges[exKey], 10*time.Second)

				// запуск воркера для отображения основыных параметров сервера
				go workers.GetExangeStateTimer(ctx, allExchanges[exKey], 10*time.Second)

				//запуск воркера для получения статистики сервера
				go workers.GetMemStatsTimer(ctx, allExchanges[exKey], 60*time.Second)
			}
		}

	}

	//------- HTTP СЕРВЕР

	//порт http сервера
	httpServerPort := os.Getenv("HTTP_SERVER_PORT")

	//переведем gin в режим релиза
	gin.SetMode(gin.ReleaseMode)

	//создадим роутер
	r := gin.New()

	//разрешаем cors
	r.Use(cors.Default())

	//главная страница
	r.GET("/ping", func(c *gin.Context) {
		c.String(200, "ok")
	})
	//информация по всем биржам и симолам
	r.GET("/api/all_exchnges_info", func(c *gin.Context) {

		slog.Debug("Получен запрос GET", "url", "/api/all_exchnges_info")

		//локальный тип для ExchangeInfo - в него может быть придется положить еще что то
		type LocalExchangeInfo struct {
			AllSymbolsInfo map[string]shared.SymbolInfo `json:"all_symbols_info"`
		}

		//инициализируем ответ в виде хеш таблицы
		exchangesData := make(map[string]LocalExchangeInfo)

		for exchangeKey := range allExchanges {

			//локальные данные по символу
			localAllSymbolsInfo := make(map[string]shared.SymbolInfo)

			//для каждого символа
			for symbolKey := range allExchanges[exchangeKey].AllSymbols {
				//запишем данные по символу в хеш таблицу
				localAllSymbolsInfo[symbolKey] = allExchanges[exchangeKey].AllSymbols[symbolKey].Info.GetInfo()
			}

			//создадим экземпляр
			var exchangeInfo LocalExchangeInfo
			exchangeInfo.AllSymbolsInfo = localAllSymbolsInfo

			//запишем в хеш таблицу биржи хеш таблицу с данные по символам
			exchangesData[exchangeKey] = exchangeInfo
		}
		c.JSON(200, exchangesData)
	})
	//информация по свечам kline
	r.POST("/api/kline_data", func(c *gin.Context) {

		//структура данных запроса
		type KlineRequest struct {
			ExchangeName string `json:"exchange_name"`
			Symbol       string `json:"symbol"`
			Contract     string `json:"contract"`
			Category     string `json:"category"`
			Interval     string `json:"interval"`
		}

		//переменная для запроса
		var req KlineRequest

		//структура даннх ответа
		type KlineResponse struct {
			ExchangeName string     `json:"exchange_name"`
			Symbol       string     `json:"symbol"`
			Contract     string     `json:"contract"`
			Category     string     `json:"category"`
			Interval     string     `json:"interval"`
			Klines       [][]string `json:"klines"`
			LastCandle   []string   `json:"last_candle"`
			Ts           int64      `json:"ts"`
		}

		// парсим данные
		if err := c.ShouldBindJSON(&req); err != nil {

			slog.Error("Ошибка в запросе: неверный формат данных", "url", "/api/kline_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: неверный формат данных"})
			return
		}

		slog.Debug("Получен запрос POST", "url", "/api/kline_data", "req", req)

		//проверим есть ли такая биржа
		exchange, ok := allExchanges[req.ExchangeName]
		if !ok {
			slog.Error("Ошибка в запросе: такой биржи не существует", "url", "/api/kline_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: такой биржи не существует"})
			return
		}
		//проверим есть ли такой символ
		_, ok = exchange.AllSymbols[req.Symbol]
		if !ok {
			slog.Error("Ошибка в запросе: такого символа не существует", "url", "/api/kline_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: такой символ не существует"})
			return
		}

		//составим начальный ответ
		response := KlineResponse{
			ExchangeName: req.ExchangeName,
			Symbol:       req.Symbol,
			Contract:     "",
			Category:     "",
			Interval:     "",
			Klines:       [][]string{},
			LastCandle:   []string{},
		}

		//получим ключ
		key, err := exchange.Client.MakeKlineKey(req.Contract, req.Category, req.Interval)
		if err != nil {
			slog.Error("Ошибка при получении ключа", "url", "/api/kline_data", "req", req)
			return
		}
		exchange.AllKlineData.RLock()
		KlineDataMu, ok := exchange.AllKlineData.KlineDataMap[key]
		exchange.AllKlineData.RUnlock()

		//если данные не найдены
		if !ok {

			slog.Warn("Данные по запросу kline не найдены", "url", "/api/kline_data", "req", req)

			//запустим получение данных kline для текущего запроса
			workers.GetKlineData(exchange, req.Symbol, req.Contract, req.Category, req.Interval)

			//запустим получение данных last_candle
			exchange.Client.WsSubscribeLastCandle(exchange, req.Contract, req.Category, req.Interval)

			//еще раз обратимя к данным они уже должны быть - предыдущая функция синхранная
			exchange.AllKlineData.RLock()
			KlineDataMu, ok := exchange.AllKlineData.KlineDataMap[key]
			exchange.AllKlineData.RUnlock()

			if !ok {
				slog.Error("Ошибка в запросе: после получения начальных данных, данные kline не находяться", "url", "/api/kline_data", "req", req)

				// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
				c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: после получения начальных данных, данные kline не находяться"})
				return

			}

			//получим данные
			KlineData := KlineDataMu.GetKlineData()

			//заполним недостающие поля
			response.Contract = KlineData.Contract
			response.Category = KlineData.Category
			response.Interval = KlineData.Interval
			response.Klines = KlineData.Klines
			response.Ts = KlineData.Ts.UnixMilli()

			//дадим ответ
			c.JSON(http.StatusOK, gin.H{
				"status": "success",
				"data":   response,
			})

		} else {
			slog.Info("Данные по запросу kline найдены", "url", "/api/kline_data", "req", req)

			//получим данные
			KlineData := KlineDataMu.GetKlineData()

			//заполним недостающие поля
			response.Contract = KlineData.Contract
			response.Category = KlineData.Category
			response.Interval = KlineData.Interval
			response.Klines = KlineData.Klines
			response.LastCandle = KlineData.LastCandle

			response.Ts = KlineData.Ts.UnixMilli()

			//дадим ответ
			c.JSON(http.StatusOK, gin.H{
				"status": "success",
				"data":   response,
			})
		}

	})
	//информация по последней свече kline
	r.POST("/api/last_candle", func(c *gin.Context) {

		//структура данных запроса
		type LastCandleRequest struct {
			ExchangeName string `json:"exchange_name"`
			Symbol       string `json:"symbol"`
			Contract     string `json:"contract"`
			Category     string `json:"category"`
			Interval     string `json:"interval"`
		}

		var req LastCandleRequest

		//структура даннх ответа
		type LastCandleResponse struct {
			LastCandle []string `json:"last_candle"`
			Ts         string   `json:"ts"`
		}

		// парсим данные
		if err := c.ShouldBindJSON(&req); err != nil {

			slog.Error("Ошибка в запросе: неверный формат данных", "url", "/api/last_candle", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: неверный формат данных"})
			return
		}

		slog.Debug("Получен запрос POST", "url", "/api/last_candle", "req", req)

		//проверим есть ли такая биржа
		exchange, ok := allExchanges[req.ExchangeName]
		if !ok {
			slog.Error("Ошибка в запросе: такой биржи не существует", "url", "/api/last_candle", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: такой биржи не существует"})
			return
		}

		//получим ключ данных kline
		key, err := exchange.Client.MakeKlineKey(req.Contract, req.Category, req.Interval)
		if err != nil {
			slog.Error("Ошибка при получении ключа", "url", "/api/last_candle", "req", req)
			return
		}

		exchange.AllKlineData.RLock()
		KlineDataMu, ok := exchange.AllKlineData.KlineDataMap[key]
		exchange.AllKlineData.RUnlock()
		if !ok {
			slog.Error("Ошибка при получении данных по ключу", "url", "/api/last_candle", "req", req, "key", key)

			//дадим пустой ответ
			c.JSON(http.StatusOK, gin.H{
				"status":      "noKlineData",
				"last_candle": "",
				"ts":          "",
			})
			return
		}

		KlineDataMu.Lock()
		//получим данные последней свечки
		lastCandle := KlineDataMu.KlineData.Klines[len(KlineDataMu.KlineData.Klines)-1]
		ts := KlineDataMu.KlineData.Ts
		//продлим время жизни данным
		KlineDataMu.KlineData.DeleteTime = time.Now().Add(exchange.FetchIntervals.KlineDeleteDeltaTime)
		KlineDataMu.Unlock()

		//дадим ответ
		c.JSON(http.StatusOK, gin.H{
			"status":      "success",
			"last_candle": lastCandle,
			"ts":          ts.UnixMilli(),
		})

		slog.Warn("Отправлена последняя свеча", "url", "/api/last_candle", "req", req, "lastCandle", lastCandle)

	})
	//текущая информация по тикерам spot, perpetual. futures. options
	r.POST("/api/tickers_data", func(c *gin.Context) {
		type TickersRequest struct {
			ExchangeName string   `json:"exchange_name" binding:"required"`
			Symbol       string   `json:"symbol" binding:"required"`
			LinesToFetch []string `json:"lines_to_fetch"`
		}

		var req TickersRequest

		// парсим данные
		if err := c.ShouldBindJSON(&req); err != nil {

			slog.Error("Ошибка в запросе: неверный формат данных", "url", "/api/tickers_data", "req", req)

			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: неверный формат данных"})
			return
		}

		slog.Debug("Получен запрос POST", "url", "/api/tickers_data", "req", req)

		//проверим есть ли такая биржа
		exchange, ok := allExchanges[req.ExchangeName]
		if !ok {
			slog.Error("Ошибка в запросе: такой биржи не существует", "url", "/api/tickers_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: такой биржи не существует"})
			return
		}
		//проверим есть ли такой символ
		_, ok = exchange.AllSymbols[req.Symbol]
		if !ok {
			slog.Error("Ошибка в запросе: такого символа не существует", "url", "/api/tickers_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: такой символ не существует"})
			return
		}

		type TickersResponse struct {
			Spot       shared.TickersDataSpot      `json:"spot"`
			Perpetual  shared.TickersDataPerpetual `json:"perpetual"`
			Futures    []shared.TickersDataFutures `json:"futures"`
			Options    []shared.TickersDataOptions `json:"options"`
			IndexPrice string                      `json:"index_price"`
			Ts         int64                       `json:"ts"`
		}

		//получим данные тикеров
		allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.Lock()
		spot := allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.Spot
		perpetual := allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.Perpetual
		futures := allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.Futures
		options := allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.Options
		indexPrice := allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.IndexPrice
		ts := allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.Ts.UnixMilli()
		allExchanges[req.ExchangeName].AllSymbols[req.Symbol].TikersData.Unlock()

		newOptionsArr := []shared.TickersDataOptions{}

		//для каждого опциона
		for i := range options {

			//доступные данные по опционным линиям
			availableOptionsLines := options[i].OptionsLines

			//тукущий набор
			newOptionsLines := shared.OptionsLines{
				Strikes: availableOptionsLines.Strikes,

				Cmark: []string{},
				Pmark: []string{},
				Cask:  []string{},
				Pask:  []string{},
				Cbid:  []string{},
				Pbid:  []string{},

				Cspread: []string{},
				Pspread: []string{},

				Cdelta: []string{},
				Pdelta: []string{},
				Cgamma: []string{},
				Pgamma: []string{},
				Cvega:  []string{},
				Pvega:  []string{},
				Ctheta: []string{},
				Ptheta: []string{},

				Coi:   []string{},
				Poi:   []string{},
				Ctv:   []string{},
				Ptv:   []string{},
				Cv24h: []string{},
				Pv24h: []string{},

				CaskIv: []string{},
				PaskIv: []string{},
				CbidIv: []string{},
				PbidIv: []string{},
				MarkIv: []string{},

				Cgex:   []string{},
				Pgex:   []string{},
				Netgex: []string{},

				CFeeM: []string{},
				PFeeM: []string{},
				CFeeT: []string{},
				PFeeT: []string{},
			}

			//для каждой линии
			for j := range req.LinesToFetch {

				switch req.LinesToFetch[j] {
				case "c_mark":
					newOptionsLines.Cmark = availableOptionsLines.Cmark
				case "p_mark":
					newOptionsLines.Pmark = availableOptionsLines.Pmark
				case "c_ask":
					newOptionsLines.Cask = availableOptionsLines.Cask
				case "p_ask":
					newOptionsLines.Pask = availableOptionsLines.Pask
				case "c_bid":
					newOptionsLines.Cbid = availableOptionsLines.Cbid
				case "p_bid":
					newOptionsLines.Pbid = availableOptionsLines.Pbid

				case "c_spread":
					newOptionsLines.Cspread = availableOptionsLines.Cspread
				case "p_spread":
					newOptionsLines.Pspread = availableOptionsLines.Pspread

				case "c_delta":
					newOptionsLines.Cdelta = availableOptionsLines.Cdelta
				case "p_delta":
					newOptionsLines.Pdelta = availableOptionsLines.Pdelta
				case "c_gamma":
					newOptionsLines.Cgamma = availableOptionsLines.Cgamma
				case "p_gamma":
					newOptionsLines.Pgamma = availableOptionsLines.Pgamma
				case "c_vega":
					newOptionsLines.Cvega = availableOptionsLines.Cvega
				case "p_vega":
					newOptionsLines.Pvega = availableOptionsLines.Pvega
				case "c_theta":
					newOptionsLines.Ctheta = availableOptionsLines.Ctheta
				case "p_theta":
					newOptionsLines.Ptheta = availableOptionsLines.Ptheta

				case "c_oi":
					newOptionsLines.Coi = availableOptionsLines.Coi
				case "p_oi":
					newOptionsLines.Poi = availableOptionsLines.Poi
				case "c_tv":
					newOptionsLines.Ctv = availableOptionsLines.Ctv
				case "p_tv":
					newOptionsLines.Ptv = availableOptionsLines.Ptv
				case "c_v24h":
					newOptionsLines.Cv24h = availableOptionsLines.Cv24h
				case "p_v24h":
					newOptionsLines.Pv24h = availableOptionsLines.Pv24h

				case "c_ask_iv":
					newOptionsLines.CaskIv = availableOptionsLines.CaskIv
				case "p_ask_iv":
					newOptionsLines.PaskIv = availableOptionsLines.PaskIv
				case "c_bid_iv":
					newOptionsLines.CbidIv = availableOptionsLines.CbidIv
				case "p_bid_iv":
					newOptionsLines.PbidIv = availableOptionsLines.PbidIv
				case "mark_iv":
					newOptionsLines.MarkIv = availableOptionsLines.MarkIv

				case "c_gex":
					newOptionsLines.Cgex = availableOptionsLines.Cgex
				case "p_gex":
					newOptionsLines.Pgex = availableOptionsLines.Pgex
				case "net_gex":
					newOptionsLines.Netgex = availableOptionsLines.Netgex

				case "c_fee_m":
					newOptionsLines.CFeeM = availableOptionsLines.CFeeM
				case "p_fee_m":
					newOptionsLines.PFeeM = availableOptionsLines.PFeeM
				case "c_fee_t":
					newOptionsLines.CFeeT = availableOptionsLines.CFeeT
				case "p_fee_t":
					newOptionsLines.PFeeT = availableOptionsLines.PFeeT
				}
			}

			//сформируем данные опционов с неообходимыми линиями
			new_options := shared.TickersDataOptions{
				Symbol:     options[i].Symbol,
				Series:     options[i].Series,
				ExpiryTime: options[i].ExpiryTime,

				MinMarkIv:             options[i].MinMarkIv,
				NearestIvToIndexPrice: options[i].NearestIvToIndexPrice,

				CallIv25Delta: options[i].CallIv25Delta,
				PutIv25Delta:  options[i].PutIv25Delta,
				SkewIv25Delta: options[i].SkewIv25Delta,

				OptionsLines: newOptionsLines,
			}

			//добавим данные опционов в общий массив
			newOptionsArr = append(newOptionsArr, new_options)
		}

		tickersResponse := TickersResponse{
			Spot:       spot,
			Perpetual:  perpetual,
			Futures:    futures,
			Options:    newOptionsArr,
			IndexPrice: indexPrice,
			Ts:         ts,
		}

		//дадим ответ
		c.JSON(http.StatusOK, gin.H{
			"status":        "success",
			"exchange_name": req.ExchangeName,
			"symbol":        req.Symbol,
			"tickers_data":  tickersResponse,
		})

		// slog.Warn("Отправлены tickers_data", "url", "/api/last_candle", "req", req, "tickers_data", tickersResponse)

	})

	server := &http.Server{
		Addr:    "127.0.0.1:" + httpServerPort,
		Handler: r,
	}

	//запуск сервера
	// r.Run("127.0.0.1:" + httpServerPort)

	// Запуск сервера в горутине
	go func() {

		slog.Info("Запуск локального сервера", "server", "http://localhost:"+httpServerPort)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {

			slog.Error("Критическая ошибка сервера", "err", err)
		}
	}()

	//обработка сигналов выхода
	<-ctx.Done()

	slog.Warn("Получен сигнал выключения! Начинаем контролируемую остановку...")

	slog.Info("Ожидаем отключение http-сервера...")

	// таймер для отключения http сервера
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	//остановка сервера
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Ошибка при остановке HTTP-сервера", "err", err)
	}
	slog.Info("http-сервер отключен")

	slog.Info("Ожидаем отключение фоновых задач...")
	time.Sleep(3 * time.Second)

	slog.Info("Работа программы завершена")
}
