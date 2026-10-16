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
	"syscall"
	"time"

	"crypto_options_screener/pkg/exchanges/bybit"
	"crypto_options_screener/pkg/shared"
	"crypto_options_screener/pkg/workers"

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
	})

	//логгер
	logger := slog.New(handler)

	//установим логгер по умолчанию
	slog.SetDefault(logger)

	// Цветной вывод
	// slog.Debug("Отладка")
	// slog.Info("Уровень инфо")
	// slog.Warn("Предупреждение")
	// slog.Error("Ошибка")

	//сокращение логов
	// info := func(msg string, args ...any) {
	// 	slog.Info(msg, args...)
	// }
	// warn := func(msg string, args ...any) {
	// 	slog.Info(msg, args...)
	// }
	// error_slog := func(msg string, args ...any) {
	// 	slog.Error(msg, args...)
	// }

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

	//настройки для всех бирж
	allExchnges := []shared.Exchange{
		{
			IsActive: true,
			Name:     "bybit",
			Symbols: []shared.Symbols{
				{Symbol: "BTC", Categories: []string{"linear", "option"}},
				{Symbol: "SPCX", Categories: []string{"linear", "option"}},
				// {Symbol: "ETH", Categories: []string{"linear", "option"}},
				// {Symbol: "SOL", Categories: []string{"linear", "option"}},
				// {Symbol: "DOGE", Categories: []string{"linear", "option"}},
				// {Symbol: "XRP", Categories: []string{"linear", "option"}},
				// {Symbol: "MNT", Categories: []string{"linear", "option"}},
				// {Symbol: "XAUT", Categories: []string{"linear", "option"}},
				// {Symbol: "HYPE", Categories: []string{"linear", "option"}},
				{Symbol: "NVDA", Categories: []string{"linear", "option"}},
				// {Symbol: "SNDK", Categories: []string{"linear", "option"}},
				// {Symbol: "QQQ", Categories: []string{"linear", "option"}},
				// {Symbol: "SKHY", Categories: []string{"linear", "option"}},
				// {Symbol: "MU", Categories: []string{"linear", "option"}},
				// {Symbol: "TSLA", Categories: []string{"linear", "optios"}},
				// {Symbol: "SOXL", Categories: []string{"linear", "option"}},
			},
			Intervals: shared.Intervals{
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

	//------ ПОЛУЧЕНИЕ И ОБРАБОТКА ДАННЫХ СО ВСЕХ БИРЖ

	//для каждой биржи
	for _, exchange := range allExchnges {

		slog.Info("Состояние биржи", "Name", exchange.Name, "IsActive", exchange.IsActive)

		//если биржа активна
		if exchange.IsActive {

			//запустим горутину для обработки получения данных
			go func() {

				switch exchange.Name {
				case "bybit":

					//создадим клиента
					exchange.Client = bybit.New("public_data", "public_data")

					slog.Info("Создан клиент для биржи", "exchangeName", exchange.Name)
				}

				//получим информацию для всех монет на бирже и сохраним в бд
				workers.GetAllSymbolsInfo(pool, &exchange)

				//получаем данные kline spot для всех монет, всех интервалов и сохраним в бд
				workers.GetKlineDataInitial(pool, &exchange, &allKlineIntervals, "spot")

				//получаем данные kline linear для всех монет, всех интервалов и сохраним в бд
				workers.GetKlineDataInitial(pool, &exchange, &allKlineIntervals, "linear")

				//запускаем горутину воркера для получения обновления данных kline по таймеру
				go workers.GetKlineDataTimer(ctx, pool, &exchange)

				//запускаем воркера для получения последних свечей
				go workers.GetKlineCandleLast(ctx, pool, &exchange, "linear")

				//запускаем воркера для получения данных по опционам по таймеру
				go workers.GetOptionsDataTimer(ctx, pool, &exchange)

				//запуск воркера для сохранения данных
				go workers.SaveSymbolDataToDbTimer(ctx, pool, &exchange)

				//запуск воркера для получения статистики сервера
				go workers.GetMemStatsTimer(ctx, &exchange)

			}()
		}

	}

	//------- HTTP СЕРВЕР

	//порт http сервера
	httpServerPort := os.Getenv("HTTP_SERVER_PORT")

	//переведем gin в режим релиза
	gin.SetMode(gin.ReleaseMode)

	//создадим роутер
	r := gin.New()

	//главная страница
	r.GET("/", func(c *gin.Context) {
		c.String(200, "Hello World")
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
