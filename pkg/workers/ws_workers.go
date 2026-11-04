package workers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"

	"crypto_options_screener/pkg/shared"
)

// запуск универсального вебсокета
func StartWebSocket(ctx context.Context, exchange *shared.Exchange, baseEndPoint string) error {
	//  канал управления подписками (буферизованный, чтобы не блокировать вызывающий код)
	cmdChan := make(chan shared.CmdChanMsg, 100)

	// подключаемся к вебсокету
	conn, _, err := websocket.DefaultDialer.Dial(baseEndPoint, nil)
	if err != nil {
		return fmt.Errorf("ошибка подключения к ws: %w", err)
	}

	slog.Info("Ws установлено соединение", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint)

	//созадим запись для для вебсокета внтури биржи
	exchange.Ws[baseEndPoint] = &shared.WsData{
		CmdChan:   cmdChan,
		TopicList: []string{},
		CmdBuffer: []shared.CmdChanMsg{},
	}

	//канал для остановки пинга
	pingStopChan := make(chan struct{})

	//запустим воркер для пинга к серверу по интервалу
	go exchange.Client.WsPingTimer(ctx, conn, pingStopChan, exchange.ExchangeName, baseEndPoint)

	//горутина для отключения воркера по контексту
	go func() {

		//блокируем горутину до события выхода
		<-ctx.Done()

		// закрываем соедиение
		conn.Close()
		slog.Info("Ws закрыт", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint)
	}()

	// Горутина для чтения сообщений (Bybit требует читать входящие данные, иначе буфер переполнится)
	go func() {
		for {

			//если контекст отменен — выходим, не пытаясь подключиться снова
			if ctx.Err() != nil {
				slog.Info("Ws linear отключен по контексту", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint)
				break
			}

			//читаем сообщение
			_, message, err := conn.ReadMessage()
			if err != nil {
				// Если контекст отменен — это штатное закрытие сокета, выходим без паники
				if ctx.Err() != nil {
					// slog.Info("Ws отключен по контексту", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint)
					break
				}

				slog.Error("Ошибка чтения ws", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "error", err)

				//остановим горутину пинга на сервер
				close(pingStopChan)

				slog.Info("Запущен таймер переподключение WebSocket", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint)

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
						conn, _, err = websocket.DefaultDialer.Dial(baseEndPoint, nil)
						if err != nil {
							slog.Error("Ошибка переподключения к ws", "exchangeName", exchange.ExchangeName, "error", err)

							//продолжим цикл for - да да continue продолжает for а если break то прерывает селект)))
							continue
						}
						//если переподключение прошло успешно

						//переопределим канал для остановки пинг воркера
						pingStopChan = make(chan struct{})

						//запустим воркер для пинга к серверу по интервалу на новом соединении и сновым каналом остановки
						go exchange.Client.WsPingTimer(ctx, conn, pingStopChan, exchange.ExchangeName, baseEndPoint)

						//текущий вебсокет
						ws := exchange.Ws[baseEndPoint]

						//копируем топики под мьютексом во временный слайс
						ws.Lock()
						topicsCopy := make([]string, len(ws.TopicList))
						copy(topicsCopy, ws.TopicList)
						ws.Unlock()

						//для каждого топика
						for _, topic := range topicsCopy {
							//сформируем команду
							newCmd := shared.CmdChanMsg{
								ID:         exchange.GenID.Next(), //там внутри тоже мьютекс
								Action:     "subscribe",
								Topic:      topic,
								DeleteTime: time.Now().Add(exchange.FetchIntervals.WsCmdMsgDeleteDelta),
							}

							// передадим нанные в канал управления
							ws.CmdChan <- newCmd
						}

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

			//любой ответ передаем обработчику сообщений конкретного клиента, он дальше разбераеться что пришло с учетом особенностей биржи
			exchange.Client.WsDecodeMessage(exchange, baseEndPoint, conn, message)
		}
	}()

	// Горутина-менеджер (управление подписками)
	go func() {
		defer conn.Close()
		slog.Info("Ws запущен канал управления подписками", "ExchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint)

		//прочитаем сообщения - они приходят по универсальному каналу
		for cmdMsg := range cmdChan {

			slog.Warn("Ws получено управляющее сообщение", "ExchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "cmdMsg", cmdMsg)

			switch exchange.ExchangeName {
			case "Bybit":

				type WsMessageBibyt struct {
					ReqID string   `json:"req_id,omitempty"`
					Op    string   `json:"op"`
					Args  []string `json:"args"`
				}

				//если поулчена команда подписаться
				if cmdMsg.Action == "subscribe" {

					msg := WsMessageBibyt{
						ReqID: cmdMsg.ID,
						Op:    cmdMsg.Action,
						Args:  []string{cmdMsg.Topic},
					}

					//запишем изначальную команду в буфер отправленных сообщений
					exchange.Ws[baseEndPoint].Lock()
					exchange.Ws[baseEndPoint].CmdBuffer = append(exchange.Ws[baseEndPoint].CmdBuffer, cmdMsg)
					exchange.Ws[baseEndPoint].Unlock()

					// Отправляем в вебсокет
					err := conn.WriteJSON(msg)
					if err != nil {
						slog.Error("Ошибка отправки запроса в WS", "cmdMsg", cmdMsg, "err", err)

					}
					slog.Info("Запрос отправлен в WS", "op", cmdMsg, "cmdMsg", cmdMsg)
				}

			}

		}
	}()

	// Возвращаем канал наружу, чтобы другие горутины могли отправлять туда команды
	return nil
}
