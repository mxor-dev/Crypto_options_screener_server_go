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
			AllSymbols: map[string]*shared.AllSymbols{
				"BTC": &shared.AllSymbols{Symbol: "BTC", FetchFilter: []string{"linear", "option"}},
				// "DOGE": &shared.AllSymbols{Symbol: "DOGE", FetchFilter: []string{"linear", "option"}},
				// "ETH":  &shared.AllSymbols{Symbol: "ETH", FetchFilter: []string{"linear", "option"}},
				// "HYPE": &shared.AllSymbols{Symbol: "HYPE", FetchFilter: []string{"linear", "option"}},
				// "MNT":  &shared.AllSymbols{Symbol: "MNT", FetchFilter: []string{"linear", "option"}},
				// "MU":   &shared.AllSymbols{Symbol: "MU", FetchFilter: []string{"linear", "option"}},
				"NVDA": &shared.AllSymbols{Symbol: "NVDA", FetchFilter: []string{"linear", "option"}},
				// "QQQ":  &shared.AllSymbols{Symbol: "QQQ", FetchFilter: []string{"linear", "option"}},
				// "SKHY": &shared.AllSymbols{Symbol: "SKHY", FetchFilter: []string{"linear", "option"}},
				// "SNDK": &shared.AllSymbols{Symbol: "SNDK", FetchFilter: []string{"linear", "option"}},
				// "SOL":  &shared.AllSymbols{Symbol: "SOL", FetchFilter: []string{"linear", "option"}},
				// "SOXL": &shared.AllSymbols{Symbol: "SOXL", FetchFilter: []string{"linear", "option"}},
				"SPCX": &shared.AllSymbols{Symbol: "SPCX", FetchFilter: []string{"linear", "option"}},
				// "TSLA": &shared.AllSymbols{Symbol: "TSLA", FetchFilter: []string{"linear", "option"}},
				// "XAUT": &shared.AllSymbols{Symbol: "XAUT", FetchFilter: []string{"linear", "option"}},
				// "XRP":  &shared.AllSymbols{Symbol: "XRP", FetchFilter: []string{"linear", "option"}},
			},
			FetchIntervals: shared.FetchIntervals{
				RestDataGetInterval:    200 * time.Millisecond,
				LinearDataGetInterval:  2 * time.Second,
				OptionsDataGetInterval: 2 * time.Second,
			},
		},
		"Mexc": {
			IsActive:     true,
			ExchangeName: "Mexc",
			AllSymbols:   map[string]*shared.AllSymbols{},
			FetchIntervals: shared.FetchIntervals{
				RestDataGetInterval:    200 * time.Millisecond,
				LinearDataGetInterval:  2 * time.Second,
				OptionsDataGetInterval: 2 * time.Second,
			},
		},
	}

	//интервалы kline
	allKlineIntervals := []string{
		"15m",
		"1h",
		"1D",
		"1W",
	}

	//для каждой биржи заполним nil map
	for _, exchangeVal := range AllExchanges {
		//для каждого символа
		for _, symbolVal := range exchangeVal.AllSymbols {

			//создадим хеш таблицы для хранения kline
			symbolVal.KlineSpot = map[string]*shared.KlineDataMu{}
			symbolVal.KlineLinear = map[string]*shared.KlineDataMu{}

			//для каждого интервала
			for i := range allKlineIntervals {
				//создадим пустую запись в хеш таблице
				symbolVal.KlineSpot[allKlineIntervals[i]] = &shared.KlineDataMu{}
				symbolVal.KlineLinear[allKlineIntervals[i]] = &shared.KlineDataMu{}
			}
		}
	}

	//------ ПОЛУЧЕНИЕ И ОБРАБОТКА ДАННЫХ СО ВСЕХ БИРЖ

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

				//получаем данные kline spot для всех монет, всех интервалов и сохраним в бд
				workers.GetKlineDataInitial(pool, exchangeVal, &allKlineIntervals, "spot")

				//получаем данные kline linear для всех монет, всех интервалов и сохраним в бд
				workers.GetKlineDataInitial(pool, exchangeVal, &allKlineIntervals, "linear")

				//запускаем горутину воркера для получения обновления данных kline по таймеру
				go workers.GetKlineDataTimer(ctx, pool, exchangeVal)

				//запускаем воркера для получения последних свечей
				go workers.GetKlineCandleLast(ctx, pool, exchangeVal, "linear")

				//запускаем воркера для получения данных по опционам по таймеру
				go workers.GetOptionsDataTimer(ctx, pool, exchangeVal)

				//запуск воркера для сохранения данных
				go workers.SaveSymbolDataToDbTimer(ctx, pool, exchangeVal)

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
			Exchange      string `json:"exchange"`
			Symbol        string `json:"symbol"`
			Category      string `json:"category"`
			KlineInterval string `json:"kline_interval"`
		}

		//переменная для запроса
		var req KlineRequest

		// парсим данные
		if err := c.ShouldBindJSON(&req); err != nil {

			slog.Error("Ошибка в данных запроса", "url", "/api/kline_data", "req", req)

			// Если прислали невалидный JSON или типы данных не совпали, возвращаем ошибку 400
			c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат запроса: " + err.Error()})
			return
		}

		slog.Debug("Получен запрос POST", "url", "/api/kline_data", "req", req)

		var data shared.KlineData

		switch req.Category {
		case "spot":
			//получим массив свечей
			data = AllExchanges[req.Exchange].AllSymbols[req.Symbol].KlineSpot[req.KlineInterval].LoadKlineData()
		case "linear":
			//получим массив свечей
			data = AllExchanges[req.Exchange].AllSymbols[req.Symbol].KlineLinear[req.KlineInterval].LoadKlineData()
		}

		//дадим ответ
		c.JSON(http.StatusOK, gin.H{
			"status": "success",
			"data":   data,
		})

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
