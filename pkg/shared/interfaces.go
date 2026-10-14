package shared

import (
	"net/http"

	"github.com/gorilla/websocket"
)

type Client interface {
	GetExchangeName() string
	GetSymbolInfo(symbol string) (SymbolInfo, error)                                                //получает данные по симвоолу
	GetKline(symbol string, category string, klineInterval string, limit string) (KlineData, error) // получает данные свечей по perpetual или споту
	GetOptions(symbol string) (OptionsData, error) //получает текущие данные по опционам для текущего символа

	WsConnect(streamType string) (*websocket.Conn, *http.Response, error)
	WsDecodePong(message []byte) (bool, error)
	WsDecodeLinearLastPrice(message []byte) (LastPriceResponse, error)

	FormTopic(streamType string, args []string) (string, error) //формирует топики в соответствии с внутренними названиями биржи
}
