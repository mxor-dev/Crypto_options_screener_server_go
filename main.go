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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Защита от утечки ресурсов

	//------- ПЕРЕХВАТ СИСТЕМНЫХ СИГНАЛОВ

	// Перехват системных сигналов Ctrl+C
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

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

	//------ ЗАПРОС ДАННЫХ

	//настройки для всех бирж
	allExchnges := []shared.Exchange{
		{
			IsActive: true,
			Name:     "bybit",
			Symbols: []string{
				"BTC",
				// "ETH",
				// "SOL",
				// "DOGE",
				// "XRP",
				// "MNT",
				// "XAUT",
				// "HYPE",
				"SPCX",
				// "NVDA",
				// "SNDK",
				// "QQQ",
				// "SKHY",
				// "MU",
				// "TSLA",
				// "SOXL",
			},
			Intervals: shared.Intervals{
				RestDataGetInterval:    200 * time.Millisecond,
				LinearDataGetInterval:  2 * time.Second,
				OptionsDataGetInterval: 2 * time.Second,
			},
		},
		{
			IsActive: false,
			Name:     "moex",
			Symbols: []string{
				"GZP",
			},
			Intervals: shared.Intervals{
				RestDataGetInterval:    200 * time.Millisecond,
				LinearDataGetInterval:  2 * time.Second,
				OptionsDataGetInterval: 2 * time.Second,
			},
		},
	}

	//интервалы
	// allKlineIntervals := []string{
	// 	"15m",
	// 	"30m",
	// 	"1h",
	// 	"1D",
	// 	"1W",
	// }

	//для каждой биржи
	for _, exchange := range allExchnges {

		slog.Info("Состояние биржи", "Name", exchange.Name, "IsActive", exchange.IsActive)

		//если биржа активна
		if exchange.IsActive {

			//запустим горутину для обработки получения данных
			go func(ctx context.Context, ex shared.Exchange) {

				switch ex.Name {
				case "bybit":

					//создадим клиента
					exchange.Client = bybit.New("public_data", "public_data")

					slog.Info("Создан клиент для биржи", "exchangeName", "bybit")
				}

				//получим информацию для всех монет на бирже
				workers.GetAllSymbolsInfo(exchange, pool)

				//получаем данные kline для всех монет и всех интервалов
				// workers.GetKlineDataInitial(exchange, pool, allKlineIntervals)

				//
				select {}

			}(ctx, exchange)
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
	<-quit

	slog.Warn("Получен сигнал выключения! Начинаем контролируемую остановку...")

	// таймер для отключения http сервера
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	//остановка сервера
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Ошибка при остановке HTTP-сервера", "err", err)
	}

	slog.Info("http-сервер отключен")

	// Б. Мгновенно даем сигнал ВСЕМ фоновым горутинам сворачивать работу
	// slog.Warn("📢 Отправляем сигнал отмены всем фоновым горутинам...")
	// cancel()

	// // В. Блокируем поток main и ждем, пока ВСЕ горутины вызовут wg.Done()
	// wg.Wait()

	slog.Info("Работа программы завершена")
}
