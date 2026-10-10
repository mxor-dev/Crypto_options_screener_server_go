package bybit

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"crypto_options_screener/pkg/convert"
	"crypto_options_screener/pkg/exchanges/exchange_client"
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
		ExchangeName: b.ExchangeName,
		Symbol:       symbol,

		SpotMinOrderQty:    "",
		SpotTickSize:       "",
		SpotPricePrecision: "",

		LinearMinOrderQty:    "",
		LinearTickSize:       "",
		LinearPricePrecision: "",

		OptionMinOrderQty:    "",
		OptionTickSize:       "",
		OptionPricePrecision: "",
	}

	//------ СПОТ

	//сформируем строку запроса для линейных инструментов
	urlSpot := fmt.Sprintf("%s/v5/market/instruments-info?symbol=%s&category=%s", baseEndPont, symbol+"USDT", "spot")

	//выполним запрос
	respSpot, err := http.Get(urlSpot)
	if err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
	}
	defer respSpot.Body.Close()

	//краткая структура ответа
	type SpotResponse struct {
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
	var dataSpot SpotResponse

	// Парсим данные в формате JSON
	if err := json.NewDecoder(respSpot.Body).Decode(&dataSpot); err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	//если есть данные по споту
	if len(dataSpot.Result.List) > 0 {
		//запишем полученные поля в result
		result.SpotMinOrderQty = dataSpot.Result.List[0].LotSizeFilter.MinOrderQty
		result.SpotTickSize = dataSpot.Result.List[0].PriceFilter.TickSize

		//найдем точность цены
		result.SpotPricePrecision = convert.GetPrecisionFromString(result.SpotTickSize)

	} else {
		slog.Warn("Нет данных по споту", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataSpot", dataSpot)
	}

	//------ ФЬЮЧЕРСЫ

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

	//если есть данные по фьючерсам
	if len(dataLinear.Result.List) > 0 {
		//запишем полученные поля в result
		result.LinearMinOrderQty = dataLinear.Result.List[0].LotSizeFilter.MinOrderQty
		result.LinearTickSize = dataLinear.Result.List[0].PriceFilter.TickSize

		//найдем точность цены
		result.LinearPricePrecision = convert.GetPrecisionFromString(result.LinearTickSize)
	} else {
		slog.Warn("Нет данных по фьючерсам", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataLinear", dataLinear)
	}

	//------ ОПЦТОНЫ

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

	//если есть данные по опционам
	if len(dataOption.Result.List) > 0 {
		//запишем полученные поля в result
		result.OptionMinOrderQty = dataOption.Result.List[0].LotSizeFilter.MinOrderQty
		result.OptionTickSize = dataOption.Result.List[0].PriceFilter.TickSize

		//найдем точность цены
		result.OptionPricePrecision = convert.GetPrecisionFromString(result.OptionTickSize)
	} else {
		slog.Warn("Нет данных по фьючерсам", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataOption", dataOption)
	}

	return result, nil
}

func (b *BybitClient) GetKline(symbol string, category string, klineInterval string, limit string) (exchange_client.KlineData, error) {

	slog.Info("Запрос GetKline", "ExchangeName", b.ExchangeName, "symbol", symbol, "category", category, "klineInterval", klineInterval)

	result := exchange_client.KlineData{
		ExchangeName: b.ExchangeName,
		Symbol:       symbol,

		Category:      category,
		KlineInterval: klineInterval,

		KlineData: [][]string{},
		DateTime:  "",
	}

	//массив для конвертации интервала
	intervals := map[string]string{
		"5m":  "5",
		"15m": "15",
		"1h":  "60",
		"1D":  "D",
		"1W":  "W",
	}

	//конвертируем интервал
	klineIntervalConverted := intervals[klineInterval]

	//сформируем строку запроса для линейных инструментов
	url := fmt.Sprintf("%s/v5/market/kline?symbol=%s&category=%s&interval=%s&limit=%s", baseEndPont, symbol+"USDT", category, klineIntervalConverted, limit)

	//выполним запрос
	respSpot, err := http.Get(url)
	if err != nil {
		return exchange_client.KlineData{}, fmt.Errorf("Сетевая ошибка: %w", err)
	}
	defer respSpot.Body.Close()

	//краткая структура ответа
	type Response struct {
		Result struct {
			Symbol string     `json:"symbol"`
			List   [][]string `json:"list"`
		} `json:"result"`
	}

	//переменная под данные сервера
	var data Response

	// Парсим данные в формате JSON
	if err := json.NewDecoder(respSpot.Body).Decode(&data); err != nil {
		return exchange_client.KlineData{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	// fmt.Printf("Получены данные: \n%+v\n", data)

	//если есть данные
	if len(data.Result.List) > 0 {
		//запишем полученные поля в result
		result.KlineData = data.Result.List
		result.DateTime = convert.GetRoundedUTCTime()
	} else {
		slog.Warn("Данные kline отсутствуют", "ExchangeName", b.ExchangeName, "symbol", symbol, "data", data)
	}

	return result, nil
}
