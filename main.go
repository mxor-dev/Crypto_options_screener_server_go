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
	AllExchanges := map[string]*shared.Exchange{
		"Bybit": {
			IsActive:     true,
			ExchangeName: "Bybit",
			AllSymbols: map[string]*shared.SymbolData{
				"BTC": {Symbol: "BTC", FetchFilter: []string{"spot", "linear", "option"}},
				// "DOGE": {Symbol: "DOGE", FetchFilter: []string{"linear", "option"}},
				// "ETH":  {Symbol: "ETH", FetchFilter: []string{"linear", "option"}},
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
				RestDataGetInterval:    200 * time.Millisecond,
				LinearDataGetInterval:  2 * time.Second,
				OptionsDataGetInterval: 2 * time.Second,
				WsDeadLineDelta:        30 * time.Second,
			},
		},
		"Mexc": {
			IsActive:     true,
			ExchangeName: "Mexc",
			AllSymbols:   map[string]*shared.SymbolData{},
			FetchIntervals: shared.FetchIntervals{
				RestDataGetInterval:    200 * time.Millisecond,
				LinearDataGetInterval:  2 * time.Second,
				OptionsDataGetInterval: 2 * time.Second,
			},
		},
	}

	//интервалы kline
	// allKlineIntervals := []string{
	// 	"15m",
	// 	"1h",
	// 	"1D",
	// 	"1W",
	// }

	// для каждой биржи
	for exchangeName := range AllExchanges {

		// Перебираем символы внутри конкретной биржи по ключу (symbolName)
		for symbolName := range AllExchanges[exchangeName].AllSymbols {

			// Получаем ПРЯМОЙ указатель на объект в мапе, чтобы менять оригинал
			symbolData := AllExchanges[exchangeName].AllSymbols[symbolName]

			// Инициализируем мапу клайнов (Мьютексы на старте лочить НЕ НУЖНО)
			symbolData.AllKlineData = shared.KlineDataMapMu{
				KlineDataMap: make(map[string]*shared.KlineDataMu),
			}

			// Инициализируем пустой слайс для опционов
			symbolData.OptionsData = []shared.OptionsData{}
		}
	}

	//дельта удаления Kline
	// KlineDeleteDeltaTime := 5 * time.Minute
	KlineDeleteDeltaTime := 1 * time.Hour

	//лимит на свечки
	KlineLimit := "1000"

	//***********************************************************
	//****** ПОЛУЧЕНИЕ И ОБРАБОТКА ДАННЫХ СО ВСЕХ БИРЖ **********
	//***********************************************************

	//для каждой биржи
	for exchangeKey, exchangeVal := range AllExchanges {

		slog.Info("Состояние биржи", "exchangeName", exchangeKey, "IsActive", exchangeVal.IsActive)

		//если биржа активна
		if exchangeVal.IsActive {

			//выберем имя биржи
			switch exchangeKey {
			case "Bybit":
				//создадим клиента
				exchangeVal.Client = bybit.New("public_data", "public_data")

				slog.Info("Создан клиент для биржи", "exchangeName", exchangeKey)
			case "Mexc":
				slog.Info("Создание клиента для биржи будет реализовано позднее", "exchangeName", exchangeKey)
			default:
				slog.Warn("Неизвестная биржа", "exchangeName", exchangeKey)
			}

			//если создан клинет для биржи
			if exchangeVal.Client != nil {
				//запускаем воркеров

				//получим информацию для всех монет на бирже и сохраним в бд
				workers.GetAllSymbolsInfo(pool, exchangeVal)

				//получаем данные kline spot для всех монет, всех интервалов
				// workers.GetKlineDataInitial(exchangeVal, &allKlineIntervals, "spot")

				//получаем данные kline linear для всех монет, всех интервалов
				// workers.GetKlineDataInitial(exchangeVal, &allKlineIntervals, "linear")

				//запускаем горутину воркера для получения обновления данных kline по таймеру
				go workers.GetKlineDataTimer(ctx, exchangeVal)

				//в зависимости от биржи для получнеия последней свечи могут использоваться один или два ws воркера
				switch exchangeKey {
				case "Bybit":
					//запускаем ws воркера для получения данных по spot
					exchangeVal.WsCmdChanSpot, err = workers.StartWebSocket(ctx, exchangeVal, "wss://stream.bybit.com/v5/public/spot")
					if err != nil {
						slog.Error("Ошибка при старте ws worker", "exchangeName", exchangeKey, "baseEndPoint", "wss://stream.bybit.com/v5/public/spot")
					}

					//запускаем ws воркера для получения данных по linear
					exchangeVal.WsCmdChanLinear, err = workers.StartWebSocket(ctx, exchangeVal, "wss://stream.bybit.com/v5/public/linear")
					if err != nil {
						slog.Error("Ошибка при старте ws worker", "exchangeName", exchangeKey, "baseEndPoint", "wss://stream.bybit.com/v5/public/linear")
					}

				case "Mexc":
					slog.Info("Запуск ws worker для биржи будет реализовано позднее", "exchangeName", exchangeKey)
				}

				//запускаем воркера для получения данных по опционам по таймеру
				// go workers.GetOptionsDataTimer(ctx, pool, exchangeVal)

				//запуск воркера для сохранения данных
				// go workers.SaveSymbolDataToDbTimer(ctx, pool, exchangeVal)

				//запуск воркера для очистки klineData
				go workers.KlineDataDeleteTimer(ctx, AllExchanges)

				//запуск воркера для получения статистики сервера
				go workers.GetMemStatsTimer(ctx, exchangeVal)
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
			AllSymbolsInfo map[string]shared.SymbolInfo
		}

		//инициализируем ответ в виде хеш таблицы
		exchangesData := make(map[string]LocalExchangeInfo)

		for exchangeKey := range AllExchanges {

			//локальные данные по символу
			localAllSymbolsInfo := make(map[string]shared.SymbolInfo)

			//для каждого символа
			for symbolKey := range AllExchanges[exchangeKey].AllSymbols {
				//запишем данные по символу в хеш таблицу
				localAllSymbolsInfo[symbolKey] = AllExchanges[exchangeKey].AllSymbols[symbolKey].Info.GetInfo()
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

		//структура даннх запроса
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
			ExchangeName string
			Symbol       string
			Contract     string
			Category     string
			Interval     string
			Klines       [][]string
			LastCandle   []string
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
		_, ok := AllExchanges[req.ExchangeName]
		if !ok {
			slog.Error("Ошибка в запросе: такой биржи не существует", "url", "/api/kline_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: такой биржи не существует"})
			return
		}
		//проверим есть ли такая биржа
		SymbolData, ok := AllExchanges[req.ExchangeName].AllSymbols[req.Symbol]
		if !ok {
			slog.Error("Ошибка в запросе: такого символа не существует", "url", "/api/kline_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка в запросе: такой символа не существует"})
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

		//составим ключ
		key := fmt.Sprintf("%s:%s:%s", req.Contract, req.Category, req.Interval)
		SymbolData.AllKlineData.RLock()
		KlineDataMu, ok := SymbolData.AllKlineData.KlineDataMap[key]
		SymbolData.AllKlineData.RUnlock()

		//если данные не найдены
		if !ok {

			slog.Warn("Данные по запросу не найдены", "url", "/api/kline_data", "req", req)

			// //создадим данные с таким ключом
			// SymbolData.AllKlineData.Lock()

			// //для каждого интервала
			// for i := range allKlineIntervals {
			// 	//смформируем ключ
			// 	key := fmt.Sprintf("%s:%s:%s", req.Contract, req.Category, allKlineIntervals[i])
			// 	//сделаем пустой объект
			// 	SymbolData.AllKlineData.KlineDataMap[key] = &shared.KlineDataMu{}

			// 	//заполним поля начальными значениями
			// 	SymbolData.AllKlineData.KlineDataMap[key].KlineData = shared.KlineData{
			// 		Contract: req.Contract,
			// 		Category: req.Category,
			// 		Interval: allKlineIntervals[i],

			// 		Klines:     [][]string{},
			// 		LastCandle: []string{},

			// 		DeleteTime: time.Now().Add(KlineDeleteDeltaTime),
			// 	}
			// }
			// SymbolData.AllKlineData.Unlock()

			// //получим данные kline начальные для всех интервалов - делаем отдельно чтобы не зависали локи мьютексов
			// workers.GetKlineDataInitial(AllExchanges[req.ExchangeName], allKlineIntervals, req.Symbol, req.Contract, req.Category, KlineLimit)

			//создадим данные с таким ключом
			SymbolData.AllKlineData.Lock()

			//смформируем ключ
			key := fmt.Sprintf("%s:%s:%s", req.Contract, req.Category, req.Interval)
			//сделаем пустой объект
			SymbolData.AllKlineData.KlineDataMap[key] = &shared.KlineDataMu{}

			//заполним поля начальными значениями
			SymbolData.AllKlineData.KlineDataMap[key].KlineData = shared.KlineData{
				Contract: req.Contract,
				Category: req.Category,
				Interval: req.Interval,

				Klines:     [][]string{},
				LastCandle: []string{},

				DeleteTime: time.Now().Add(KlineDeleteDeltaTime),
			}
			SymbolData.AllKlineData.Unlock()

			workers.GetKlineData(AllExchanges[req.ExchangeName], req.Symbol, req.Contract, req.Category, req.Interval, KlineLimit)

			//еще раз обратимя к данным они уже должны быть

			SymbolData.AllKlineData.RLock()
			KlineDataMu, ok := SymbolData.AllKlineData.KlineDataMap[key]
			SymbolData.AllKlineData.RUnlock()

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
			response.LastCandle = KlineData.LastCandle

			//дадим ответ
			c.JSON(http.StatusOK, gin.H{
				"status": "success",
				"data":   response,
			})

		} else {
			slog.Info("Данные по запросу найдены", "url", "/api/kline_data", "req", req)

			//получим данные
			KlineData := KlineDataMu.GetKlineData()

			//заполним недостающие поля
			response.Contract = KlineData.Contract
			response.Category = KlineData.Category
			response.Interval = KlineData.Interval
			response.Klines = KlineData.Klines
			response.LastCandle = KlineData.LastCandle

			//дадим ответ
			c.JSON(http.StatusOK, gin.H{
				"status": "success",
				"data":   response,
			})
		}

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
