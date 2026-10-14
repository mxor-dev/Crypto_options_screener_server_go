package bybit

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"

	"crypto_options_screener/pkg/convert"
	"crypto_options_screener/pkg/shared"

	"github.com/gorilla/websocket"
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

// получает ися биржи из клиента
func (b *BybitClient) GetExchangeName() string {
	return b.ExchangeName
}

// получает информацию по всем символам
func (b *BybitClient) GetSymbolInfo(symbol string) (shared.SymbolInfo, error) {

	// slog.Info("Запрос данных SymbolInfo", "fun", "GetSymbolInfo", "ExchangeName", b.ExchangeName, "symbol", symbol)

	//ответ
	result := shared.SymbolInfo{
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
		return shared.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
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
		return shared.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	//если есть данные по споту
	if len(dataSpot.Result.List) > 0 {
		//запишем полученные поля в result
		result.SpotMinOrderQty = dataSpot.Result.List[0].LotSizeFilter.MinOrderQty
		result.SpotTickSize = dataSpot.Result.List[0].PriceFilter.TickSize

		//найдем точность цены
		result.SpotPricePrecision = convert.GetPrecisionFromString(result.SpotTickSize)

	} else {
		slog.Warn("Нет данных по споту", "fun", "GetSymbolInfo", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataSpot", dataSpot)
	}

	//------ ФЬЮЧЕРСЫ

	//сформируем строку запроса для линейных инструментов
	urlLinear := fmt.Sprintf("%s/v5/market/instruments-info?symbol=%s&category=%s", baseEndPont, symbol+"USDT", "linear")

	//выполним запрос
	respLinear, err := http.Get(urlLinear)
	if err != nil {
		return shared.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
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
		return shared.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
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
		slog.Warn("Нет данных по фьючерсам", "fun", "GetSymbolInfo", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataLinear", dataLinear)
	}

	//------ ОПЦТОНЫ

	//софрмируем url для опционов
	urlOptions := fmt.Sprintf("%s/v5/market/instruments-info?baseCoin=%s&category=%s", baseEndPont, symbol, "option")

	respOptions, err := http.Get(urlOptions)
	if err != nil {
		return shared.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
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
		return shared.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
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

// получает данные kline
func (b *BybitClient) GetKline(symbol string, category string, klineInterval string, limit string) (shared.KlineData, error) {

	// slog.Info("Запрос данных kline", "fun", "GetKline", "ExchangeName", b.ExchangeName, "symbol", symbol, "category", category, "klineInterval", klineInterval)

	result := shared.KlineData{
		ExchangeName: b.ExchangeName,
		Symbol:       symbol,

		Category:      category,
		KlineInterval: klineInterval,

		KlineData: [][]string{},
		Time:      0,
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
		return shared.KlineData{}, fmt.Errorf("Ошбика Get запроса %s: %w", url, err)
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
		return shared.KlineData{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	// fmt.Printf("Получены данные: \n%+v\n", data)

	//если есть данные
	if len(data.Result.List) > 0 {

		//массив формата TOHLC
		var TOHLC [][]string

		//для каждой свечки
		for _, candle := range data.Result.List {
			//переместим строку первые пять значений
			TOHLC = append(
				TOHLC,
				[]string{
					candle[0],
					candle[1],
					candle[2],
					candle[3],
					candle[4],
				})
		}

		//поменяем порядок свечей
		slices.Reverse(TOHLC)

		//запишем полученные поля в result
		result.KlineData = TOHLC
		result.Time = convert.GetRoundedUTCTimeMs()

		slog.Info("Получены данные kline", "fun", "GetKline", "ExchangeName", b.ExchangeName, "symbol", symbol, "category", category, "klineInterval", klineInterval, "len(result.KlineData)", len(result.KlineData))

	} else {
		slog.Warn("Данные kline отсутствуют", "ExchangeName", b.ExchangeName, "symbol", symbol, "category", category, "klineInterval", klineInterval, "data", data)
	}

	return result, nil
}

// получает данные по опционам
func (b *BybitClient) GetOptions(symbol string) (shared.OptionsData, error) {

	result := shared.OptionsData{
		Symbol:                    symbol,
		Series:                    "",
		MinMarkIv:                 "",
		NearestMarkIvToIndexPrice: "",
		OptionsLines:              [][]string{},
		TimeToExpiry:              "",
	}

	//сформируем строку запроса для линейных инструментов
	url := fmt.Sprintf("%s/v5/market/tickers?baseCoin=%s&category=%s", baseEndPont, symbol, "option")

	//выполним запрос
	respOptions, err := http.Get(url)
	if err != nil {
		return shared.OptionsData{}, fmt.Errorf("Ошбика Get запроса %s: %w", url, err)
	}
	defer respOptions.Body.Close()

	//структура каждого опциона
	type OptionItem struct {
		Symbol                 string `json:"symbol"`
		Bid1Price              string `json:"bid1Price"`
		Bid1Size               string `json:"bid1Size"`
		Bid1Iv                 string `json:"bid1Iv"`
		Ask1Price              string `json:"ask1Price"`
		Ask1Size               string `json:"ask1Size"`
		Ask1Iv                 string `json:"ask1Iv"`
		LastPrice              string `json:"lastPrice"`
		HighPrice24h           string `json:"highPrice24h"`
		LowPrice24h            string `json:"lowPrice24h"`
		MarkPrice              string `json:"markPrice"`
		IndexPrice             string `json:"indexPrice"`
		MarkIv                 string `json:"markIv"`
		UnderlyingPrice        string `json:"underlyingPrice"`
		OpenInterest           string `json:"openInterest"`
		Turnover24h            string `json:"turnover24h"`
		Volume24h              string `json:"volume24h"`
		TotalVolume            string `json:"totalVolume"`
		TotalTurnover          string `json:"totalTurnover"`
		Delta                  string `json:"delta"`
		Gamma                  string `json:"gamma"`
		Vega                   string `json:"vega"`
		Theta                  string `json:"theta"`
		PredictedDeliveryPrice string `json:"predictedDeliveryPrice"`
		Change24h              string `json:"change24h"`
	}

	//короткая структура для ответа
	type OptionsResponse struct {
		Result struct {
			List []OptionItem `json:"list"`
		} `json:"result"`
	}

	//вычитываем все сырые байты из тела ответа
	bodyBytes, err := io.ReadAll(respOptions.Body)
	if err != nil {
		return shared.OptionsData{}, fmt.Errorf("Ошибка чтения тела ответа %s: %w", url, err)
	}

	//экземпляр структуры
	var rawOptionsData OptionsResponse

	//распаковываем JSON-байты в созданный объект Go (передаем по указателю &)
	err = json.Unmarshal(bodyBytes, &rawOptionsData)
	if err != nil {
		return shared.OptionsData{}, fmt.Errorf("Ошибка парсинга JSON опционов: %w", err)
	}

	slog.Warn("Получены данные по опционам", "ExchangeName", b.ExchangeName, "symbol", symbol, "rawOptionsData", rawOptionsData)

	return result, nil
}

// создает подключение по вебсокету
func (b *BybitClient) WsConnect(streamType string) (*websocket.Conn, *http.Response, error) {

	var url string

	//выберем url для подключения
	switch streamType {
	case "ws-spot-channel":
		url = "wss://stream.bybit.com/v5/public/spot"
	case "ws-linear-channel":
		url = "wss://stream.bybit.com/v5/public/linear"
	default:
		return nil, nil, fmt.Errorf("Неизвестный streamType: %s", streamType)
	}

	//создаем подключение
	conn, resp, err := websocket.DefaultDialer.Dial(url, nil)

	return conn, resp, err
}

// декодирует ответ понг сервера
func (b *BybitClient) WsDecodePong(message []byte) (bool, error) {
	// Краткая структура для извлечения только признаков pong
	type PongResponse struct {
		RetMsg string `json:"ret_msg"`
		Op     string `json:"op"`
	}

	var resp PongResponse

	// Десериализуем JSON. Если это рыночные данные другого формата,
	// unmarshal может выдать ошибку или просто оставить поля пустыми.
	if err := json.Unmarshal(message, &resp); err != nil {
		return false, err
	}

	// Проверяем, совпадает ли хотя бы одно поле с признаком pong
	if resp.RetMsg == "pong" {
		return true, nil
	}

	return false, nil
}

// вытаскивает из сообщения знгачение lastPrice
func (b *BybitClient) WsDecodeLinearLastPrice(message []byte) (shared.LastPriceResponse, error) {

	// краткая структура
	type Response struct {
		Topic string `json:"topic"`
		Data  struct {
			Symbol    string `json:"symbol"`
			LastPrice string `json:"lastPrice"`
		} `json:"data"`
	}

	//переменная структуры
	var resp Response

	//десериализуем JSON и в случае ошибки возвращаем её
	if err := json.Unmarshal(message, &resp); err != nil {
		return shared.LastPriceResponse{}, err
	}

	//результат работы
	result := shared.LastPriceResponse{
		Symbol:    resp.Data.Symbol,
		LastPrice: resp.Data.LastPrice,
	}

	return result, nil
}

// формирует топик для потока и аргументов
func (b *BybitClient) FormTopic(streamType string, args []string) (string, error) {

	//в зависимости от типа потока
	switch streamType {
	case "ws-spot-channel":
		return fmt.Sprintf("tickers.%sUSDT", args[0]), nil
	case "ws-linear-channel":
		return fmt.Sprintf("tickers.%sUSDT", args[0]), nil
	default:
		return "", fmt.Errorf("Неизвестный streamType: %s", streamType)
	}

}
