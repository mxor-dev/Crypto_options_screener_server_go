package bybit

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"crypto_options_screener/exchange_client"
)

// базовая точка доступа
const baseEndPont = "https://api.bybit.com"

// ------- СТРУКТУРА
type BybitClient struct {
	ExchangeName string
	apiKey       string
	apiSecret    string
}

// ------- КОНСТРУКТОР
func New(apiKey, apiSecret string) *BybitClient {
	return &BybitClient{
		ExchangeName: "bybit",
		apiKey:       apiKey,
		apiSecret:    apiSecret,
	}
}

//------- МЕТОДЫ

func (b *BybitClient) GetExchangeName() string {
	return b.ExchangeName
}

func (b *BybitClient) GetSymbolInfo(symbol string) (exchange_client.SymbolInfo, error) {

	slog.Info("Запрос GetSymbolInfo", "ExchangeName", b.ExchangeName, "symbol", symbol)

	//ответ
	result := exchange_client.SymbolInfo{
		Symbol: "BTC",

		LinearMinOrderQty: "",
		LinearTickSize:    "",

		OptionMinOrderQty: "",
		OptionTickSize:    "",
	}

	//сформируем строку запроса для линейных инструментов
	urlLinear := fmt.Sprintf("%s/v5/market/instruments-info?symbol=%s&category=%s", baseEndPont, symbol+"USDT", "linear")

	//выполним запрос
	respLinear, err := http.Get(urlLinear)
	if err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
	}
	defer respLinear.Body.Close()

	//краткая структура ответа
	type LinearResponse struct {
		Result struct {
			List []struct {
				Symbol      string `json:"symbol"`
				PriceFilter struct {
					TickSize string `json:"tickSize"`
				} `json:"priceFilter"`
				LotSizeFilter struct {
					MinOrderQty string `json:"minOrderQty"`
				} `json:"lotSizeFilter"`
			} `json:"list"`
		} `json:"result"`
	}

	//переменная под данные сервера
	var dataLinear LinearResponse

	// Парсим данные в формате JSON
	if err := json.NewDecoder(respLinear.Body).Decode(&dataLinear); err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	// fmt.Printf("dataLinear:\n%+v\n", dataLinear)

	//запишем полученные поля в result
	result.LinearMinOrderQty = dataLinear.Result.List[0].LotSizeFilter.MinOrderQty
	result.LinearTickSize = dataLinear.Result.List[0].PriceFilter.TickSize

	//софрмируем url для опционов
	urlOptions := fmt.Sprintf("%s/v5/market/instruments-info?baseCoin=%s&category=%s", baseEndPont, symbol, "option")

	respOptions, err := http.Get(urlOptions)
	if err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
	}
	defer respOptions.Body.Close()

	//краткая структура
	type OptionResponse struct {
		Result struct {
			List []struct {
				Symbol      string `json:"symbol"`
				PriceFilter struct {
					TickSize string `json:"tickSize"`
				} `json:"priceFilter"`
				LotSizeFilter struct {
					MinOrderQty string `json:"minOrderQty"`
				} `json:"lotSizeFilter"`
			} `json:"list"`
		} `json:"result"`
	}

	//переменная под данные сервера
	var dataOption OptionResponse

	// Парсим данные в формате JSON
	if err := json.NewDecoder(respOptions.Body).Decode(&dataOption); err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	// fmt.Printf("dataOption:\n%+v\n", dataOption)

	//запишем полученные поля в result
	result.OptionMinOrderQty = dataOption.Result.List[0].LotSizeFilter.MinOrderQty
	result.OptionTickSize = dataOption.Result.List[0].PriceFilter.TickSize

	return result, nil
}
