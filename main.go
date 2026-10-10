package main

import (
	"context"
	"crypto_options_screener/bybit"
	"crypto_options_screener/exchange_client"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
)

// интервалы запроса данных
type Intervals struct {
	RestDataGetInterval    time.Duration
	LinearDataGetInterval  time.Duration
	OptionsDataGetInterval time.Duration
}

// биржа
type Exchange struct {
	IsActive  bool
	Name      string
	Symbols   []string
	Intervals Intervals
}

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

	//------ ЗАПРОС ДАННЫХ

	//настройки для всех бирж
	all_exchanges := []Exchange{
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
			Intervals: Intervals{
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
			Intervals: Intervals{
				RestDataGetInterval:    200 * time.Millisecond,
				LinearDataGetInterval:  2 * time.Second,
				OptionsDataGetInterval: 2 * time.Second,
			},
		},
	}

	//для каждой биржи
	for _, exchange := range all_exchanges {

		slog.Info("Состояние биржи", "Name", exchange.Name, "IsActive", exchange.IsActive)

		//если биржа активна
		if exchange.IsActive {

			//запустим горутину для обработки получения данных
			go func(ctx context.Context, ex Exchange) {

				//создадим клиента для
				var client exchange_client.Client

				switch ex.Name {
				case "bybit":

					//создадим клиента
					client = bybit.New("public_data", "public_data")
				}

				slog.Info("Создан клиент для биржи", "ExchangeName", client.GetExchangeName())

				var allSymbolsInfo []exchange_client.SymbolInfo

				//для каждого символа для текущей биржи
				for _, symbol := range ex.Symbols {

					//получим данные для текущей монетки
					symbolInfo, err := client.GetSymbolInfo(symbol)
					if err != nil {
						slog.Error("Ошибка при получении symbolInfo", "ExchangeName", client.GetExchangeName(), "symbol", symbol)
					}

					slog.Info("Получен symbolInfo", "ExchangeName", client.GetExchangeName(), "symbol", symbol, "symbolInfo", symbolInfo)

					//добавим данные по символы в глобальный массив
					allSymbolsInfo = append(allSymbolsInfo, symbolInfo)

					//сделаем паузу между запросами
					time.Sleep(ex.Intervals.RestDataGetInterval)
				}

				slog.Info("Итоговый allSymbolsInfo", "ExchangeName", client.GetExchangeName(), "allSymbolsInfo", allSymbolsInfo)

				select {}

			}(ctx, exchange)
		}

	}

	//------- HTTP СЕРВЕР

	//порт http сервера
	httpServerPort := os.Getenv("httpServerPort")

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
