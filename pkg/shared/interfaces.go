package shared

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type Client interface {
	GetExchangeName() string
	GetSymbolInfo(symbol string, RestDataGetInterval time.Duration) (SymbolInfo, error)           //получает данные по симвоолу
	GetKline(contract string, category string, interval string, limit string) ([][]string, error) // получает данные свечей по perpetual или споту
	GetOptions(symbol string) ([]OptionsData, error)                                              //получает текущие данные по опционам для текущего символа

	WsConnect(streamType string) (*websocket.Conn, *http.Response, error)
	WsDecodePong(message []byte) (bool, error)
	WsDecodeLinearLastPrice(message []byte) (LastPriceResponse, error)

	FormTopic(streamType string, args []string) (string, error) //формирует топики в соответствии с внутренними названиями биржи
}
