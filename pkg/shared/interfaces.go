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

	WsPingTimer(ctx context.Context, exchange *Exchange, stopChan chan struct{}, wsKey string) // функция проверки пинга разная в зависимости от биржи
	WsDecodeMessage(exchange *Exchange, wsKey string, conn *websocket.Conn, message []byte)
	WsSubscribeLastCandle(exchange *Exchange, contract string, category string, interval string) //подписывается на канал получения последней свечи

	MakeKlineKey(contract string, category string, interval string) (string, error) //формирует ключ для данных kline

	// WsFormTopicGetTrades(cmdMsg CmdChanMsg) (string, error) //формирует топики для получения последних сделок
	// WsDecodePong(message []byte) (bool, error)
	// WsDecodeLinearLastPrice(message []byte) (LastPriceResponse, error)
}
