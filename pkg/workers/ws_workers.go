package workers

import (
	"context"
	"crypto_options_screener/pkg/shared"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

// вспомогательный воркер для отправики сообщений ping
func WsPingTimer(ctx context.Context, conn *websocket.Conn, stopChan chan struct{}, exchangeName string) {

	slog.Info("Запуск воркера WsPingTimer", "exchangeName", exchangeName)

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

// запускает произвольного воркера для получения данных с определенного канала
func WsWorkerStart(ctx context.Context, exchange *shared.Exchange, streamType string, topicArr []string, dataChan chan []byte) {

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
