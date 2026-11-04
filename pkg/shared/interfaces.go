package shared

import (
	"context"
	"time"

	"github.com/gorilla/websocket"
)

type Client interface {
	GetExchangeName() string
	GetBaseEndpointSpot() string
	GetBaseEndpointLinear() string
	ConvertInterval(interval string) (string, error)                                              //конвертирует внешние интервалы во внутренние
	GetSymbolInfo(symbol string, RestDataGetInterval time.Duration) (SymbolInfo, error)           //получает данные по симвоолу
	GetKline(contract string, category string, interval string, limit string) ([][]string, error) // получает данные свечей по perpetual или споту
	GetOptions(symbol string) ([]OptionsData, error)                                              //получает текущие данные по опционам для текущего символа

	WsPingTimer(ctx context.Context, conn *websocket.Conn, stopChan chan struct{}, exchangeName string, baseEndPoint string) // функция проверки пинга разная в зависимости от биржи
	WsDecodeMessage(exchange *Exchange, baseEndPoint string, conn *websocket.Conn, message []byte)                           // обработка сообщение и передача данных в общее хранилище

	MakeKlineKey(contract string, category string, interval string) (string, error) //формирует ключ для данных kline
	// WsFormTopicGetTrades(cmdMsg CmdChanMsg) (string, error) //формирует топики для получения последних сделок
	// WsDecodePong(message []byte) (bool, error)
	// WsDecodeLinearLastPrice(message []byte) (LastPriceResponse, error)
}
