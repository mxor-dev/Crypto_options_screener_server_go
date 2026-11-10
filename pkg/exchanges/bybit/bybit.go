package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"crypto_options_screener/pkg/convert"
	"crypto_options_screener/pkg/shared"

	"github.com/gorilla/websocket"
)

// базовая точка доступа
const baseEndPont = "https://api.bybit.com"

// ------- СТРУКТУРА КЛИЕНТА ------
type BybitClient struct {
	ExchangeName         string
	apiKey               string
	apiSecret            string
	Intervals            map[string]string
	WsBaseEndpointSpot   string
	WsBaseEndpointLinear string
}

// ------- СТРУКТУРS ДЛЯ WS ------

// Краткая структура для извлечения только признаков pong
// type WsPongResponse struct {
// 	RetMsg string `json:"ret_msg"`
// 	Op     string `json:"op"`
// }

// структура ответ на запрос subscribe/unsubscribe
type WsBybitBaseResponse struct {
	Success bool            `json:"success"` // Статус выполнения: true / false
	RetMsg  string          `json:"ret_msg"` // Возвращаемое сообщение (например, "subscribe" или текст ошибки)
	ConnID  string          `json:"conn_id"` // Уникальный ID вашего текущего WebSocket соединения
	ReqID   string          `json:"req_id"`  // пользоовательский ID который был в изначальном запросе
	Op      string          `json:"op"`      // Название операции, на которую пришел ответ (например, "subscribe")
	Topic   string          `json:"topic"`
	Ts      int64           `json:"ts"`
	Type    string          `json:"type"` // "snapshot" или "delta"
	Data    json.RawMessage `json:"data"`
}

// type WsBybitDataResponse struct {
// 	Topic string          `json:"topic"`
// 	Ts    int64           `json:"ts"`
// 	Type  string          `json:"type"` // "snapshot" или "delta"
// 	Data  json.RawMessage `json:"data"`
// }

type WsBybitTradeItem struct {
	ID        string `json:"i"`   // ID сделки
	Timestamp int64  `json:"T"`   // Время сделки (Unix timestamp в миллисекундах)
	Price     string `json:"p"`   // Цена сделки (прилетает строкой, например "77719.3")
	Volume    string `json:"v"`   // Объем сделки (прилетает строкой, например "0.000643")
	Side      string `json:"S"`   // Сторона: "Buy" или "Sell"
	Seq       int64  `json:"seq"` // Последовательный номер (Sequence)
	Symbol    string `json:"s"`   // Торговая пара, например "BTCUSDT"
	BT        bool   `json:"BT"`  // Является ли блочной сделкой (Block Trade)
	RPI       bool   `json:"RPI"` // Розничный индекс (Retail Price Index)
}
type WsBybitTradeItemShort struct {
	Price string `json:"p"` // Цена сделки (прилетает строкой, например "77719.3")
}

type WsBybitKlineItem struct {
	Start int64  `json:"start"` // Timestamp начала свечи (мс) -> T
	Open  string `json:"open"`  // Цена открытия -> O
	High  string `json:"high"`  // Максимальная цена -> H
	Low   string `json:"low"`   // Минимальная цена -> L
	Close string `json:"close"` // Цена закрытия -> C
}

// ------- КОНСТРУКТОР ------
func New(apiKey string, apiSecret string) *BybitClient {
	return &BybitClient{
		ExchangeName: "Bybit",
		apiKey:       apiKey,
		apiSecret:    apiSecret,
		Intervals: map[string]string{
			"5m":  "5",
			"15m": "15",
			"1h":  "60",
			"1D":  "D",
			"1W":  "W",
		},
		WsBaseEndpointSpot:   "wss://stream.bybit.com/v5/public/spot",
		WsBaseEndpointLinear: "wss://stream.bybit.com/v5/public/linear",
	}
}

//------- ОБЯЗАТЕЛЬНЫЕ ФУНКЦИИ ИНТЕРФЕЙСА ------

// получает ися биржи из клиента
func (b *BybitClient) GetExchangeName() string {
	return b.ExchangeName
}

// конвертирует внешние интервалы во внутренние
func (b *BybitClient) ConvertInterval(interval string) (string, error) {

	result, ok := b.Intervals[interval]
	if !ok {
		return "", fmt.Errorf("Ошибка конвертации интервала для биржи %s: неизвестный интервал %s", b.ExchangeName, interval)
	}

	return result, nil
}

// получает базовую точку для вебсокета
func (b *BybitClient) GetBaseEndpointSpot() string {
	return b.WsBaseEndpointSpot
}

// получает базовую точку для вебсокета
func (b *BybitClient) GetBaseEndpointLinear() string {
	return b.WsBaseEndpointLinear
}

// получает информацию по всем символам
func (b *BybitClient) GetSymbolInfo(symbol string, RestDataGetInterval time.Duration) (shared.SymbolInfo, error) {

	// slog.Info("Запрос данных SymbolInfo", "fun", "GetSymbolInfo", "ExchangeName", b.ExchangeName, "symbol", symbol)

	//ответ - пропишем чтобы потом не было ошибок с nil
	result := shared.SymbolInfo{
		ExchangeName: b.ExchangeName,
		Symbol:       symbol,

		SpotContractName:   "",
		SpotMinOrderQty:    "",
		SpotTickSize:       "",
		SpotPricePrecision: "",

		PerpetualContractName:   "",
		PerpetualMinOrderQty:    "",
		PerpetualTickSize:       "",
		PerpetualPricePrecision: "",

		FuturesContractNameArr: []string{},
		FuturesMinOrderQty:     "",
		FuturesTickSize:        "",
		FuturesPricePrecision:  "",

		// OptionContractNameArr: []string{},
		OptionsMinOrderQty:    "",
		OptionsTickSize:       "",
		OptionsPricePrecision: "",

		IsSpotAvailable:      false,
		IsPerpetualAvailable: false,
		IsFuturesAvailable:   false,
		IsOptionsAvailable:   false,
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

	//краткая структура ответа linear
	type LinearResponse struct {
		Result struct {
			List []struct {
				Symbol       string `json:"symbol"`
				ContractType string `json:"contractType"`
				Status       string `json:"status"`
				PriceFilter  struct {
					TickSize string `json:"tickSize"`
				} `json:"priceFilter"`
				LotSizeFilter struct {
					MinOrderQty string `json:"minOrderQty"`
				} `json:"lotSizeFilter"`
			} `json:"list"`
		} `json:"result"`
	}

	//переменная под данные сервера
	var dataSpot LinearResponse

	// Парсим данные в формате JSON
	if err := json.NewDecoder(respSpot.Body).Decode(&dataSpot); err != nil {
		return shared.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	//если есть данные по споту
	if len(dataSpot.Result.List) > 0 {
		//запишем полученные поля в result
		result.SpotContractName = dataSpot.Result.List[0].Symbol
		result.SpotMinOrderQty = dataSpot.Result.List[0].LotSizeFilter.MinOrderQty
		result.SpotTickSize = dataSpot.Result.List[0].PriceFilter.TickSize

		//найдем точность цены
		result.SpotPricePrecision = convert.GetPrecisionFromString(result.SpotTickSize)

		//льметим что нашли спот
		result.IsSpotAvailable = true

	} else {
		// slog.Warn("Нет данных spot", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataSpot", dataSpot)
		slog.Warn("Нет данных spot", "ExchangeName", b.ExchangeName, "symbol", symbol)

	}

	//сделаем паузу между запросами
	time.Sleep(RestDataGetInterval)

	//------ ФЬЮЧЕРСЫ

	//сформируем строку запроса для линейных инструментов
	urlLinear := fmt.Sprintf("%s/v5/market/instruments-info?baseCoin=%s&category=%s", baseEndPont, symbol, "linear")

	//выполним запрос
	respLinear, err := http.Get(urlLinear)
	if err != nil {
		return shared.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
	}
	defer respLinear.Body.Close()

	//переменная под данные сервера
	var dataLinear LinearResponse

	// Парсим данные в формате JSON
	if err := json.NewDecoder(respLinear.Body).Decode(&dataLinear); err != nil {
		return shared.SymbolInfo{}, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	// fmt.Printf("dataLinear:\n%+v\n", dataLinear)

	//если есть данные по фьючерсам
	if len(dataLinear.Result.List) > 0 {

		//для каждой записи
		for i := range dataLinear.Result.List {

			//в зависимости от типа контракта
			switch dataLinear.Result.List[i].ContractType {
			case "LinearPerpetual":

				//запишем полученные поля в result
				result.PerpetualContractName = dataLinear.Result.List[i].Symbol
				result.PerpetualMinOrderQty = dataLinear.Result.List[i].LotSizeFilter.MinOrderQty
				result.PerpetualTickSize = dataLinear.Result.List[i].PriceFilter.TickSize

				//найдем точность цены
				result.PerpetualPricePrecision = convert.GetPrecisionFromString(result.PerpetualTickSize)

				//отметим что данные найдены
				result.IsPerpetualAvailable = true

				// } else {
			case "LinearFutures":

				//если не заполнено еще ни одного имени - заполним поля один раз
				if len(result.FuturesContractNameArr) == 0 {
					//запишем полученные поля в result
					result.FuturesMinOrderQty = dataLinear.Result.List[i].LotSizeFilter.MinOrderQty
					result.FuturesTickSize = dataLinear.Result.List[i].PriceFilter.TickSize

					//найдем точность цены
					result.FuturesPricePrecision = convert.GetPrecisionFromString(result.PerpetualTickSize)

					//отметим что данные найдены
					result.IsFuturesAvailable = true
				}

				//если контракт действуюбщий
				if dataLinear.Result.List[i].Status == "Trading" {
					//запишем символ контракта
					result.FuturesContractNameArr = append(result.FuturesContractNameArr, dataLinear.Result.List[i].Symbol)
				}
			}
		}

		if result.PerpetualContractName == "" {
			// slog.Warn("Нет данных perpetual", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataLinear", dataLinear)
			slog.Warn("Нет данных perpetual", "ExchangeName", b.ExchangeName, "symbol", symbol)
		}
		if len(result.FuturesContractNameArr) == 0 {
			// slog.Warn("Нет данных futures", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataLinear", dataLinear)
			slog.Warn("Нет данных futures", "ExchangeName", b.ExchangeName, "symbol", symbol)
		}
	}

	//сделаем паузу между запросами
	time.Sleep(RestDataGetInterval)

	//------ ОПЦИОНЫ

	//сформируем url для опционов
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
		result.OptionsMinOrderQty = dataOption.Result.List[0].LotSizeFilter.MinOrderQty
		result.OptionsTickSize = dataOption.Result.List[0].PriceFilter.TickSize

		//найдем точность цены
		result.OptionsPricePrecision = convert.GetPrecisionFromString(result.OptionsTickSize)

		//отметим что данные найдены
		result.IsOptionsAvailable = true

		//для каждого опциона
		// for i := range dataOption.Result.List {
		// 	//запишем имя контракта
		// 	result.OptionContractNameArr = append(result.OptionContractNameArr, dataOption.Result.List[i].Symbol)
		// }
	} else {
		// slog.Warn("Нет данных options", "ExchangeName", b.ExchangeName, "symbol", symbol, "dataOption", dataOption)
		slog.Warn("Нет данных options", "ExchangeName", b.ExchangeName, "symbol", symbol)
	}

	return result, nil
}

// получает данные kline
func (b *BybitClient) GetKline(contract string, category string, interval string, limit string) ([][]string, error) {

	slog.Info("Запрос данных kline", "fun", "GetKline", "ExchangeName", b.ExchangeName, "contract", contract, "category", category, "interval", interval)

	//конвертируем интервал
	intervalConverted := b.Intervals[interval]

	//конвертируем категории
	categoryConverted := category
	if category == "perpetual" || category == "futures" {
		categoryConverted = "linear"
	}

	//сформируем строку запроса для линейных инструментов
	url := fmt.Sprintf("%s/v5/market/kline?symbol=%s&category=%s&interval=%s&limit=%s", baseEndPont, contract, categoryConverted, intervalConverted, limit)

	//выполним запрос
	respSpot, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("Ошбика Get запроса %s: %w", url, err)
	}
	defer respSpot.Body.Close()

	//читаем байты ответа в переменную
	bodyBytes, err := io.ReadAll(respSpot.Body)
	if err != nil {
		return nil, fmt.Errorf("Ошибка чтения ответа сервера %s: %w", url, err)
	}

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
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return nil, fmt.Errorf("Ошибка парсинга: %w", err)
	}

	// fmt.Printf("Получены данные: \n%+v\n", data)

	//если массив свечей нулевой
	if len(data.Result.List) == 0 {
		// Завершаем работу функции и выдаем ошибку с полной строкой отсета
		return nil, fmt.Errorf("В ответе биржи отсутствуют свечи len(data.Result.List) == 0. Полный ответ сервера: %s", string(bodyBytes))
	}

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

	slog.Info("Получены данные kline", "ExchangeName", b.ExchangeName, "contract", contract, "category", category, "interval", interval, "len(result.KlineData)", len(TOHLC))

	return TOHLC, nil
}

// получает данные по опционам
func (b *BybitClient) GetTickersDataSequental(exchange *shared.Exchange) {

	// var spotTikers shared.SpotTickers
	// var perpetualTikers shared.PerpetualTickers
	// var futuresTickers shared.FuturesTickers
	// var OptionsTickers shared.OptionsTickers

	//*********** Spot **************

	//для каждого символа
	for symbol := range exchange.AllSymbols {

		//получим информацию по символу
		exchange.AllSymbols[symbol].Info.Lock()
		symbolInfo := exchange.AllSymbols[symbol].Info.SymbolInfo
		exchange.AllSymbols[symbol].Info.Unlock()

		//если данные доступны
		if symbolInfo.IsSpotAvailable {

			// slog.Info("Запрос данных spot", "exchangeName", exchange.ExchangeName, "symbol", symbol+"USDT", "category", "spot")

			//сформируем строку запроса options
			url := fmt.Sprintf("%s/v5/market/tickers?symbol=%s&category=%s", baseEndPont, symbol+"USDT", "spot")

			//выполним запрос
			resp, err := http.Get(url)
			if err != nil {
				slog.Error("Ошбика Get запроса %s: %w", url, err)
				return
			}
			defer resp.Body.Close()

			type SpotTickersResponse struct {
				Result struct {
					Category string `json:"category"`
					List     []struct {
						Symbol   string `json:"symbol"`
						BidPrice string `json:"bid1Price"` // Лучшая цена покупки (лучший Bid)
						BidSize  string `json:"bid1Size"`  // Объём лучшей покупки (Bid Size)
						AskPrice string `json:"ask1Price"` // Лучшая цена продажи (лучший Ask)
						AskSize  string `json:"ask1Size"`  // Объём лучшей продажи (Ask Size)
					} `json:"list"`
				} `json:"result"`
			}

			// создаем переменную для ответа
			var spotTickers SpotTickersResponse

			// декодируем прямо из сетевого потока
			if err := json.NewDecoder(resp.Body).Decode(&spotTickers); err != nil {
				slog.Error("Ошибка парсинга JSON spot", "url", url, "error", err)
				return
			}

			list := spotTickers.Result.List
			// slog.Warn("Ответ", "list", list)
			//если в полученных данных длина массива больше 1
			if len(list) > 0 {
				//сохраним данные
				exchange.AllSymbols[symbol].TikersData.Lock()
				exchange.AllSymbols[symbol].TikersData.Spot = shared.TickersDataSpot{
					Symbol:   list[0].Symbol,
					BidPrice: list[0].BidPrice,
					BidSize:  list[0].BidSize,
					AskPrice: list[0].AskPrice,
					AskSize:  list[0].AskSize,
				}
				exchange.AllSymbols[symbol].TikersData.Unlock()

				slog.Info("Получены данные spotTickers", "exchangeName", exchange.ExchangeName, "symbol", symbol+"USDT", "category", "spot")
			}

			//сделаем паузу между запросами
			time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
		}

	}

	//*********** linear **************

	for symbol := range exchange.AllSymbols {

		//получим информацию по символу
		exchange.AllSymbols[symbol].Info.Lock()
		symbolInfo := exchange.AllSymbols[symbol].Info.SymbolInfo
		exchange.AllSymbols[symbol].Info.Unlock()

		//если данные доступны
		if symbolInfo.IsPerpetualAvailable {

			// slog.Info("Запрос данных всех linear", "exchangeName", exchange.ExchangeName, "category", "linear")

			//сформируем строку запроса options
			// url = fmt.Sprintf("%s/v5/market/tickers?symbol=%s&category=%s", baseEndPont, symbol+"USDT", "linear")
			url := fmt.Sprintf("%s/v5/market/tickers?category=%s", baseEndPont, "linear")

			//выполним запрос
			resp, err := http.Get(url)
			if err != nil {
				slog.Error("Ошбика Get запроса %s: %w", url, err)
				return
			}
			defer resp.Body.Close()

			type LinearTickersResponse struct {
				Result struct {
					Category string `json:"category"`
					List     []struct {
						Symbol   string `json:"symbol"`
						BidPrice string `json:"bid1Price"` // Лучшая цена покупки (лучший Bid)
						BidSize  string `json:"bid1Size"`  // Объём лучшей покупки (Bid Size)
						AskPrice string `json:"ask1Price"` // Лучшая цена продажи (лучший Ask)
						AskSize  string `json:"ask1Size"`  // Объём лучшей продажи (Ask Size)
					} `json:"list"`
				} `json:"result"`
			}

			// создаем переменную для ответа
			var linearTickers LinearTickersResponse

			// декодируем прямо из сетевого потока
			if err := json.NewDecoder(resp.Body).Decode(&linearTickers); err != nil {
				slog.Error("Ошибка парсинга JSON spot", "url", url, "error", err)
				return
			}

			list := linearTickers.Result.List
			// slog.Warn("Ответ", "list", list)
			//если в полученных данных длина массива больше 1
			if len(list) > 0 {

				//получим список фьючерсов для данной монеты
				exchange.AllSymbols[symbol].Info.Lock()
				FuturesContractNameArr := exchange.AllSymbols[symbol].Info.SymbolInfo.FuturesContractNameArr
				exchange.AllSymbols[symbol].Info.Unlock()

				slog.Info("Список фьючерсов", "exchangeName", exchange.ExchangeName, "symbol", symbol, "FuturesContractNameArr", FuturesContractNameArr)

				//локальный массив для данных фьючерсов
				localFuturesTickersArr := []shared.TickersDataFutures{}

				//для каждого элемента списка
				for i := range list {

					//если это фьючерс perpetual
					if list[i].Symbol == symbol+"USDT" {
						// slog.Warn("Perpetual найден")

						//сохраним данные
						exchange.AllSymbols[symbol].TikersData.Lock()
						exchange.AllSymbols[symbol].TikersData.Perpetual = shared.TickersDataPerpetual{
							Symbol:   list[i].Symbol,
							BidPrice: list[i].BidPrice,
							BidSize:  list[i].BidSize,
							AskPrice: list[i].AskPrice,
							AskSize:  list[i].AskSize,
						}
						exchange.AllSymbols[symbol].TikersData.Unlock()

					}

					if slices.Contains(FuturesContractNameArr, list[i].Symbol) {
						// slog.Warn("futures найден")

						//создадим локальные данные
						localTickers := shared.TickersDataFutures{
							Symbol:   list[i].Symbol,
							BidPrice: list[i].BidPrice,
							BidSize:  list[i].BidSize,
							AskPrice: list[i].AskPrice,
							AskSize:  list[i].AskSize,
						}

						localFuturesTickersArr = append(localFuturesTickersArr, localTickers)
					}
				}

				//если найдены фьючерсы
				if len(localFuturesTickersArr) > 0 {
					exchange.AllSymbols[symbol].TikersData.Lock()
					exchange.AllSymbols[symbol].TikersData.Futures = localFuturesTickersArr
					exchange.AllSymbols[symbol].TikersData.Unlock()

				}

				slog.Info("Получены данные linearTickers", "exchangeName", exchange.ExchangeName, "symbol", symbol, "category", "linear")
			}

			//сделаем паузу между запросами
			time.Sleep(exchange.FetchIntervals.RestDataGetInterval)
		}
	}

	//*********** Options **************

	//для каждого символа
	for symbol := range exchange.AllSymbols {

		//получим информацию по символу
		exchange.AllSymbols[symbol].Info.Lock()
		symbolInfo := exchange.AllSymbols[symbol].Info.SymbolInfo
		exchange.AllSymbols[symbol].Info.Unlock()

		//если данные доступны
		if symbolInfo.IsOptionsAvailable {

			//сформируем строку запроса options
			url := fmt.Sprintf("%s/v5/market/tickers?baseCoin=%s&category=%s", baseEndPont, symbol, "option")

			//выполним запрос
			resp, err := http.Get(url)
			if err != nil {
				slog.Error("Ошбика Get запроса %s: %w", url, err)
				return
			}
			defer resp.Body.Close()

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

			// создаем переменную для ответа
			var optionsTickers OptionsResponse

			// декодируем прямо из сетевого потока
			if err := json.NewDecoder(resp.Body).Decode(&optionsTickers); err != nil {
				slog.Error("Ошибка парсинга JSON options", "url", url, "error", err)
				return
			}

			list := optionsTickers.Result.List
			// slog.Warn("Ответ", "list", list)

			//цену индекса возьмем из первого элемента - они пои идее везде должны быть одинаковы
			indexPrice, err := strconv.ParseFloat(list[0].IndexPrice, 64)
			if err != nil {
				indexPrice = 0.0
			}

			// slog.Warn("Получены данные по опционам", "ExchangeName", b.ExchangeName, "symbol", symbol, "rawOptionsData", rawOptionsData)

			//страйк и дата - иначе страйк опциона теряется
			type StrikeAndData struct {
				Strike string
				Data   OptionItem
			}

			//структура колов и путов отдельно
			type CallsAndPuts struct {
				Calls []StrikeAndData
				Puts  []StrikeAndData
			}

			//структура для сортировки колов и путов по серии
			allSeriesMap := make(map[string]CallsAndPuts)

			//для каждого опциона
			for i := range list {

				//разобъем Symbol на части
				parts := strings.Split(list[i].Symbol, "-")

				//переменные
				currSeries := parts[1]
				if len(currSeries) == 6 {
					currSeries = "0" + currSeries
				}
				strike := parts[2]
				optionType := parts[3]

				//если серия опциона отсутствует
				if _, ok := allSeriesMap[currSeries]; !ok {
					//добавим новую пустую серию
					allSeriesMap[currSeries] = CallsAndPuts{}
				}

				// slog.Warn("Тип опциона", "ExchangeName", b.ExchangeName, "symbol", symbol, "parts[3]", parts[3])

				//достанем текущую серию из map
				currSeriesData := allSeriesMap[currSeries]

				//проверим тип опциона
				switch optionType {
				case "C":
					// slog.Warn("Добавлен Call")
					//добавим опцион к Calls

					//создадим локальную структуру опциона
					localData := StrikeAndData{
						Strike: strike,
						Data:   list[i],
					}

					//запишем данные опциона в Calls
					currSeriesData.Calls = append(currSeriesData.Calls, localData)
				case "P":
					// slog.Warn("Добавлен Put")

					//создадим локальную структуру опциона
					localData := StrikeAndData{
						Strike: strike,
						Data:   list[i],
					}

					//запишем данные опциона в Puts
					currSeriesData.Puts = append(currSeriesData.Puts, localData)
				}

				//записываем обновленную серию обратно в map
				allSeriesMap[currSeries] = currSeriesData

				// slog.Warn("Получены данные по опционам", "ExchangeName", b.ExchangeName, "symbol", symbol, "currSeriesData", currSeriesData)

			}

			// slog.Warn("Получены данные по опционам", "ExchangeName", b.ExchangeName, "symbol", symbol, "allSeriesMap", allSeriesMap)

			var localAllOptionsTickers []shared.TickersDataOptions

			//для каждой серии
			for series := range allSeriesMap {

				//найдем время до экспирации
				if len(series) != 7 {
					slog.Error("Неверная длина строки series", "error", err, "series", series)
					return
				}

				// Приводим "05NOV26" к "05Nov26" для корректного парсинга шаблоном Go
				formattedSeries := strings.ToUpper(series[:2]) + strings.Title(strings.ToLower(series[2:5])) + series[5:]

				// Парсим дату по шаблону DDMMMYY
				parsedDate, err := time.Parse("02Jan06", formattedSeries)
				if err != nil {
					slog.Error("Ошибка парсинга даты", "error", err)
					return
				}

				// Устанавливаем точное время экспирации — 08:00:00 UTC
				expiryTime := time.Date(
					parsedDate.Year(), parsedDate.Month(), parsedDate.Day(),
					8, 0, 0, 0, time.UTC,
				)

				//если серия опционов действующая
				if time.Now().Before(expiryTime) {
					//массив трайков
					var allStrikes []string

					//соберем все страйки их опционов Call
					for j := range allSeriesMap[series].Calls {
						allStrikes = append(allStrikes, allSeriesMap[series].Calls[j].Strike)
					}

					//сортируем страйки по возрастанию
					slices.SortFunc(allStrikes, func(a, b string) int {
						valA, _ := strconv.ParseFloat(a, 64)
						valB, _ := strconv.ParseFloat(b, 64)

						if valA < valB {
							return -1 // a должно идти перед b
						}
						if valA > valB {
							return 1 // a должно идти после b
						}
						return 0 // значения равны
					})

					// slog.Warn("Получены страйки", "ExchangeName", b.ExchangeName, "series", series, "allStrikes", allStrikes)

					//длина массива всех страйков
					lenAllStrikes := len(allStrikes)

					//инициализируем локальную структуру с длиной данных lenAllStrikes
					localOptionsLines := shared.OptionsLines{
						Strikes: make([]string, lenAllStrikes),

						Cmark: make([]string, lenAllStrikes),
						Pmark: make([]string, lenAllStrikes),
						Cask:  make([]string, lenAllStrikes),
						Pask:  make([]string, lenAllStrikes),
						Cbid:  make([]string, lenAllStrikes),
						Pbid:  make([]string, lenAllStrikes),

						Cspread: make([]string, lenAllStrikes),
						Pspread: make([]string, lenAllStrikes),

						Cdelta: make([]string, lenAllStrikes),
						Pdelta: make([]string, lenAllStrikes),
						Cgamma: make([]string, lenAllStrikes),
						Pgamma: make([]string, lenAllStrikes),
						Cvega:  make([]string, lenAllStrikes),
						Pvega:  make([]string, lenAllStrikes),
						Ctheta: make([]string, lenAllStrikes),
						Ptheta: make([]string, lenAllStrikes),

						Coi:   make([]string, lenAllStrikes),
						Poi:   make([]string, lenAllStrikes),
						Ctv:   make([]string, lenAllStrikes),
						Ptv:   make([]string, lenAllStrikes),
						Cv24h: make([]string, lenAllStrikes),
						Pv24h: make([]string, lenAllStrikes),

						CaskIv: make([]string, lenAllStrikes),
						PaskIv: make([]string, lenAllStrikes),
						CbidIv: make([]string, lenAllStrikes),
						PbidIv: make([]string, lenAllStrikes),
						MarkIv: make([]string, lenAllStrikes),

						Cgex:   make([]string, lenAllStrikes),
						Pgex:   make([]string, lenAllStrikes),
						Netgex: make([]string, lenAllStrikes),

						CFeeM: make([]string, lenAllStrikes),
						PFeeM: make([]string, lenAllStrikes),
						CFeeT: make([]string, lenAllStrikes),
						PFeeT: make([]string, lenAllStrikes),
					}

					//передадим страйки
					copy(localOptionsLines.Strikes, allStrikes)

					//для каждого опциона call
					for j := range allSeriesMap[series].Calls {

						//индекс страйка
						var strikeIndex int

						//для каждого страйка в данных по страйкам
						for k := range localOptionsLines.Strikes {

							//если страйки совпадают
							if allSeriesMap[series].Calls[j].Strike == localOptionsLines.Strikes[k] {
								//запишем индекс страйка
								strikeIndex = k
								break
							}
						}

						//перепишем данные Call опциона в соответствии с индексом
						localOptionsLines.Cmark[strikeIndex] = allSeriesMap[series].Calls[j].Data.MarkPrice
						localOptionsLines.Cask[strikeIndex] = allSeriesMap[series].Calls[j].Data.Ask1Price
						localOptionsLines.Cbid[strikeIndex] = allSeriesMap[series].Calls[j].Data.Bid1Price

						localOptionsLines.Cdelta[strikeIndex] = allSeriesMap[series].Calls[j].Data.Delta
						localOptionsLines.Cgamma[strikeIndex] = allSeriesMap[series].Calls[j].Data.Gamma
						localOptionsLines.Cvega[strikeIndex] = allSeriesMap[series].Calls[j].Data.Vega
						localOptionsLines.Ctheta[strikeIndex] = allSeriesMap[series].Calls[j].Data.Theta

						localOptionsLines.Coi[strikeIndex] = allSeriesMap[series].Calls[j].Data.OpenInterest
						localOptionsLines.Ctv[strikeIndex] = allSeriesMap[series].Calls[j].Data.TotalVolume
						localOptionsLines.Cv24h[strikeIndex] = allSeriesMap[series].Calls[j].Data.Volume24h

						localOptionsLines.CaskIv[strikeIndex] = allSeriesMap[series].Calls[j].Data.Ask1Iv
						localOptionsLines.CbidIv[strikeIndex] = allSeriesMap[series].Calls[j].Data.Bid1Iv

						//передадим линию MarkIv
						localOptionsLines.MarkIv[strikeIndex] = allSeriesMap[series].Calls[j].Data.MarkIv

						//спарсим MarkPrice
						markPrice, err := strconv.ParseFloat(localOptionsLines.Cmark[strikeIndex], 64)
						if err != nil {
							markPrice = 0.0
						}

						// Считаем комиссию (получаем float64)
						feeM := math.Min((float64(exchange.FeeRates.Options.MakerFeeRate)/100)*indexPrice, (float64(exchange.FeeRates.Options.MaxPartRate)/100)*markPrice)
						feeT := math.Min((float64(exchange.FeeRates.Options.TakerFeeRate)/100)*indexPrice, (float64(exchange.FeeRates.Options.MaxPartRate)/100)*markPrice)

						// Конвертируем float64 в string и сохраняем (в формате с плавающей точкой, авто-точность)
						localOptionsLines.CFeeM[strikeIndex] = strconv.FormatFloat(feeM, 'f', -1, 64)
						localOptionsLines.CFeeT[strikeIndex] = strconv.FormatFloat(feeT, 'f', -1, 64)

					}

					//для каждого опциона put
					for j := range allSeriesMap[series].Puts {

						//индекс страйка
						var strikeIndex int

						//для каждого страйка в данных по страйкам
						for k := range localOptionsLines.Strikes {
							//если страйки совпадают
							if allSeriesMap[series].Puts[j].Strike == localOptionsLines.Strikes[k] {
								//запишем индекс страйка
								strikeIndex = k
								break
							}
						}

						//перепишем данные Put опциона в соответствии с индексом
						localOptionsLines.Pmark[strikeIndex] = allSeriesMap[series].Puts[j].Data.MarkPrice
						localOptionsLines.Pask[strikeIndex] = allSeriesMap[series].Puts[j].Data.Ask1Price
						localOptionsLines.Pbid[strikeIndex] = allSeriesMap[series].Puts[j].Data.Bid1Price

						localOptionsLines.Pdelta[strikeIndex] = allSeriesMap[series].Puts[j].Data.Delta
						localOptionsLines.Pgamma[strikeIndex] = allSeriesMap[series].Puts[j].Data.Gamma
						localOptionsLines.Pvega[strikeIndex] = allSeriesMap[series].Puts[j].Data.Vega
						localOptionsLines.Ptheta[strikeIndex] = allSeriesMap[series].Puts[j].Data.Theta

						localOptionsLines.Poi[strikeIndex] = allSeriesMap[series].Puts[j].Data.OpenInterest
						localOptionsLines.Ptv[strikeIndex] = allSeriesMap[series].Puts[j].Data.TotalVolume
						localOptionsLines.Pv24h[strikeIndex] = allSeriesMap[series].Puts[j].Data.Volume24h

						localOptionsLines.PaskIv[strikeIndex] = allSeriesMap[series].Puts[j].Data.Ask1Iv
						localOptionsLines.PbidIv[strikeIndex] = allSeriesMap[series].Puts[j].Data.Bid1Iv

						//MarkIv не пишем

						//спарсим MarkPrice
						markPrice, err := strconv.ParseFloat(localOptionsLines.Pmark[strikeIndex], 64)
						if err != nil {
							markPrice = 0.0
						}

						// Считаем комиссию (получаем float64)
						feeM := math.Min((float64(exchange.FeeRates.Options.MakerFeeRate)/100)*indexPrice, (float64(exchange.FeeRates.Options.MaxPartRate)/100)*markPrice)
						feeT := math.Min((float64(exchange.FeeRates.Options.TakerFeeRate)/100)*indexPrice, (float64(exchange.FeeRates.Options.MaxPartRate)/100)*markPrice)

						// Конвертируем float64 в string и сохраняем (в формате с плавающей точкой, авто-точность)
						localOptionsLines.PFeeM[strikeIndex] = strconv.FormatFloat(feeM, 'f', -1, 64)
						localOptionsLines.PFeeT[strikeIndex] = strconv.FormatFloat(feeT, 'f', -1, 64)

					}

					// slog.Warn("Получены страйки", "ExchangeName", b.ExchangeName, "series", series, "localOptionsLines", localOptionsLines)

					//минимум линии и IV для 25 дельты
					var minMarkIvStr [2]string
					var nearestIvToIndexPrice [2]string
					var callIv25DeltaStr [2]string
					var putIv25DeltaStr [2]string
					var skewIv25DeltaStr string

					//если линия MarkIv не нулевая
					if len(localOptionsLines.MarkIv) > 0 {

						//первое значение макс float64
						minMarkIv := math.MaxFloat64

						// slog.Warn("Debug", "ExchangeName", b.ExchangeName, "symbol", symbol, "localOptionsLines.MarkIv", localOptionsLines.MarkIv)
						//********** minMarkIv **********

						//найдем minMarkIv
						for j := range localOptionsLines.MarkIv {

							// slog.Warn("Debug", "ExchangeName", b.ExchangeName, "symbol", symbol, "markIvStr", markIvStr)

							// Конвертируем строку в число (база 10, размер int)
							num, err := strconv.ParseFloat(localOptionsLines.MarkIv[j], 64)
							if err != nil {
								num = 0.0
							}

							// Если текущее число меньше сохраненного минимума, обновляем минимум
							if num < minMarkIv {
								//сохраним числовое значение
								minMarkIv = num
								//сохраним значение в массиве строк
								minMarkIvStr = [2]string{localOptionsLines.Strikes[j], localOptionsLines.MarkIv[j]}
							}
						}

						//********** nearestIvToIndexPrice **********

						strikeFloat, err := strconv.ParseFloat(localOptionsLines.Strikes[0], 64)
						if err != nil {
							strikeFloat = 0.0
						}

						//дельта между текущим страйком и ценой индекса
						lastIndexPriceDelta := math.Abs(indexPrice - strikeFloat)

						//найдем nearestIvToIndexPrice
						for j := range localOptionsLines.Strikes {

							//конвертируем текущий страйк
							strikeFloat, err := strconv.ParseFloat(localOptionsLines.Strikes[j], 64)
							if err != nil {
								strikeFloat = 0.0
							}

							//ищем дельту
							currIndexPriceDelta := math.Abs(indexPrice - strikeFloat)

							// Если текущая дельта меньше последней
							if currIndexPriceDelta < lastIndexPriceDelta {
								//запишем индекс и значение iv
								nearestIvToIndexPrice = [2]string{localOptionsLines.Strikes[j], localOptionsLines.MarkIv[j]}
								//обновим последнюю дельту
								lastIndexPriceDelta = currIndexPriceDelta
							}
						}

						//********* callIv25DeltaStr putIv25DeltaStr skewIv25DeltaStr ***********

						//начальные значения
						callIv25DeltaStr = [2]string{localOptionsLines.Strikes[0], localOptionsLines.MarkIv[0]}
						putIv25DeltaStr = [2]string{localOptionsLines.Strikes[0], localOptionsLines.MarkIv[0]}

						//перевод в дельту в числа
						cdeltaFloat, err := strconv.ParseFloat(localOptionsLines.Cdelta[0], 64)
						if err != nil {
							cdeltaFloat = 0.0
						}
						pdeltaFloat, err := strconv.ParseFloat(localOptionsLines.Pdelta[0], 64)
						if err != nil {
							pdeltaFloat = 0.0
						}

						//начальные дельты для путов и колов
						lastCallDeltaDelta := math.Abs(0.25 - cdeltaFloat)
						lastPutDeltaDelta := math.Abs(0.25 - pdeltaFloat)

						for j := range localOptionsLines.Strikes {
							//перевод в числа
							cdeltaFloat, err = strconv.ParseFloat(localOptionsLines.Cdelta[j], 64)
							if err != nil {
								// Обработка ошибки: если в строке прилетел мусор, задаем дефолтное значение
								cdeltaFloat = 0.0
							}
							pdeltaFloat, err = strconv.ParseFloat(localOptionsLines.Pdelta[j], 64)
							if err != nil {
								// Обработка ошибки: если в строке прилетел мусор, задаем дефолтное значение
								pdeltaFloat = 0.0
							}

							//текущие дельты для путов и колов
							currCallDeltaDelta := math.Abs(0.25 - cdeltaFloat)
							currPutDeltaDelta := math.Abs(0.25 - pdeltaFloat)

							//если текущая дельта меньше предыдущей
							if currCallDeltaDelta < lastCallDeltaDelta {

								//запишем текущие значения MarkIv
								callIv25DeltaStr = [2]string{localOptionsLines.Strikes[j], localOptionsLines.MarkIv[j]}

								//поменяем последнее значение
								lastCallDeltaDelta = currCallDeltaDelta
							}
							if currPutDeltaDelta < lastPutDeltaDelta {
								//запишем текущие значения MarkIv
								putIv25DeltaStr = [2]string{localOptionsLines.Strikes[j], localOptionsLines.MarkIv[j]}

								//поменяем последнее значение
								lastPutDeltaDelta = currPutDeltaDelta
							}

						}

						//перевод полученных MarkIv в числа
						call25DeltaIv, err := strconv.ParseFloat(callIv25DeltaStr[1], 64)
						if err != nil {
							call25DeltaIv = 0.0
						}
						put25DeltaIv, err := strconv.ParseFloat(putIv25DeltaStr[1], 64)
						if err != nil {
							put25DeltaIv = 0.0
						}

						//просчитаем skew
						skewIv25DeltaStr = strconv.FormatFloat(call25DeltaIv-put25DeltaIv, 'f', 4, 64)

					}

					//получим gex
					for j := range localOptionsLines.Strikes {
						// Парсим Call OI и Call Gamma
						cOi, err := strconv.ParseFloat(localOptionsLines.Coi[j], 64)
						if err != nil {
							cOi = 0.0
						}
						cGamma, err := strconv.ParseFloat(localOptionsLines.Cgamma[j], 64)
						if err != nil {
							cGamma = 0.0
						}

						// Парсим Put OI и Put Gamma
						pOi, err := strconv.ParseFloat(localOptionsLines.Poi[j], 64)
						if err != nil {
							pOi = 0.0
						}
						pGamma, err := strconv.ParseFloat(localOptionsLines.Pgamma[j], 64)
						if err != nil {
							pGamma = 0.0
						}

						// Считаем GEX значения
						callGex := cOi * cGamma
						putGex := -(pOi * pGamma) // Путы берутся со знаком минус
						netGex := callGex + putGex

						// Форматируем float64 обратно в string и записываем в результирующую структуру
						localOptionsLines.Cgex[j] = strconv.FormatFloat(callGex, 'f', 4, 64)
						localOptionsLines.Pgex[j] = strconv.FormatFloat(putGex, 'f', 4, 64)
						localOptionsLines.Netgex[j] = strconv.FormatFloat(netGex, 'f', 4, 64)
					}

					//просчитаем спреды
					for j := range localOptionsLines.Strikes {

						// Парсим Call Ask и Call Bid
						cAsk, cAskErr := strconv.ParseFloat(localOptionsLines.Cask[j], 64)
						cBid, cBidErr := strconv.ParseFloat(localOptionsLines.Cbid[j], 64)

						// Парсим Put Ask и Put Bid
						pAsk, pAskErr := strconv.ParseFloat(localOptionsLines.Pask[j], 64)
						pBid, pBidErr := strconv.ParseFloat(localOptionsLines.Pbid[j], 64)

						var cSpread, pSpread float64

						// Считаем Call спред, только если обе строки успешно распарсились
						if cAskErr == nil && cBidErr == nil {
							cSpread = cAsk - cBid
						}

						// Считаем Put спред, только если обе строки успешно распарсились
						if pAskErr == nil && pBidErr == nil {
							pSpread = pAsk - pBid
						}

						// Форматируем float64 обратно в string и записываем в результирующую структуру
						localOptionsLines.Cspread[j] = strconv.FormatFloat(cSpread, 'f', 4, 64)
						localOptionsLines.Pspread[j] = strconv.FormatFloat(pSpread, 'f', 4, 64)
					}

					//заполним данные для текущей серии опционов
					localOptionsTickers := shared.TickersDataOptions{
						Symbol:     symbol,
						Series:     series,
						ExpiryTime: strconv.FormatInt(expiryTime.UnixMilli(), 10),

						MinMarkIv:             minMarkIvStr,
						NearestIvToIndexPrice: nearestIvToIndexPrice,
						CallIv25Delta:         callIv25DeltaStr,
						PutIv25Delta:          putIv25DeltaStr,
						SkewIv25Delta:         skewIv25DeltaStr,

						OptionsLines: localOptionsLines,
					}

					localAllOptionsTickers = append(localAllOptionsTickers, localOptionsTickers)
				}

			}

			//сортируем итоговый результат по полю series по возрастанию времени доставки
			slices.SortFunc(localAllOptionsTickers, func(a, b shared.TickersDataOptions) int {
				timeA, errA := convert.SeriesToDeliveryTimeMs(a.Series)
				timeB, errB := convert.SeriesToDeliveryTimeMs(b.Series)

				if errA != nil {
					return -1
				}
				if errB != nil {
					return 1
				}

				if timeA < timeB {
					return -1
				}
				if timeA > timeB {
					return 1
				}

				return 0

			})

			//запишем полученные данные
			exchange.AllSymbols[symbol].TikersData.Lock()
			exchange.AllSymbols[symbol].TikersData.Options = localAllOptionsTickers
			exchange.AllSymbols[symbol].TikersData.IndexPrice = strconv.FormatFloat(indexPrice, 'f', 4, 64)
			exchange.AllSymbols[symbol].TikersData.Ts = time.Now()
			OptionsLen := len(exchange.AllSymbols[symbol].TikersData.Options)
			// slog.Info("Получены данные Option", "ExchangeName", b.ExchangeName, "symbol", symbol, "exchange.AllSymbols[symbol].Tikers", exchange.AllSymbols[symbol].Tikers.Options)
			exchange.AllSymbols[symbol].TikersData.Unlock()

			slog.Info("Получены данные Option", "ExchangeName", b.ExchangeName, "symbol", symbol, "OptionsLen", OptionsLen)
		}
	}
}

// вспомогательный воркер для отправики сообщений ping
func (b *BybitClient) WsPingTimer(ctx context.Context, exchange *shared.Exchange, stopChan chan struct{}, wsKey string) {

	slog.Info("Ws ping запуск воркера", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)

	// Создаем тикер с интервалом в 20 секунд
	ticker := time.NewTicker(20 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Ws ping таймер остановлен", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)
	}()

	for {
		select {
		case <-ticker.C:

			// составим сообщение ping
			cmdMsg := shared.CmdChanMsg{
				ID:         "",
				Action:     "ping",
				Topic:      "",
				DeleteTime: time.Now(),
			}

			// Отправляем сообщение в канал управления ws
			exchange.Ws[wsKey].CmdChan <- cmdMsg

			slog.Debug("Ws на сервер отправлен запрос ping", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)
		case <-stopChan:

			slog.Info("Ws ping воркер остановлен по сигналу", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)
			return // Выходим по внешнемук сигналу

		case <-ctx.Done():
			slog.Info("Ws ping воркер остановлен по контксту", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)
			return // Выходим по контексту приложения
		}
	}
}

// обработка сообщение и передача данных в общее хранилище
func (b *BybitClient) WsDecodeMessage(exchange *shared.Exchange, wsKey string, conn *websocket.Conn, message []byte) {
	// //еcли ответ - пустое ообщение - это переподключение
	// if string(message) == "" {
	// 	slog.Debug("Ws переподключен", "ExchangeName", exchange.ExchangeName)
	// } else {

	//базовый ответ
	var resp WsBybitBaseResponse
	if err := json.Unmarshal(message, &resp); err != nil {
		slog.Error("Ws ошибка базового парсинга", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "err", err)
		return
	}

	// switch resp.RetMsg {
	switch resp.Op {
	case "ping":
		// если это сообщение pong

		slog.Debug("Ws от сервера получен ответ pong", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)

		//передвинем дедлайн
		err := conn.SetReadDeadline(time.Now().Add(exchange.FetchIntervals.WsDeadLineDelta))
		if err != nil {
			slog.Error("Ws ошибка при декодировании pong", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "error", err)
		}

		return

	case "subscribe":
		//если это ответ на сообщение о подписке
		// {\"success\":true,\"ret_msg\":\"subscribe\",\"conn_id\":\"dbhf1usmqckqr6btpve0-120x6\",\"req_id\":\"2\",\"op\":\"subscribe\"}"

		//если подписка успешно оформлена
		if resp.Success {

			slog.Warn("Ws получено сообщение об успешной подписке", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)

			//возьмем вебсокет
			ws := exchange.Ws[wsKey]
			ws.Lock()

			//найдем топик по id
			var topic string
			var idxToDelete int
			isFound := false
			for i := range ws.CmdBuffer {
				if ws.CmdBuffer[i].ID == resp.ReqID {
					topic = ws.CmdBuffer[i].Topic
					idxToDelete = i
					isFound = true
				}
			}
			if isFound {
				//добавим топик в список топиков
				ws.TopicList = append(ws.TopicList, topic)
				//удалим команду из списка
				ws.CmdBuffer = slices.Delete(ws.CmdBuffer, idxToDelete, idxToDelete+1)
			}

			ws.Unlock()

			return
		}
	case "unsubscribe":
		// {\"success\":true,\"ret_msg\":\"unsubscribe\",\"conn_id\":\"dbhf3h3sfisbbk4i4b3g-17shw\",\"req_id\":\"2\",\"op\":\"unsubscribe\"}

		//если подписка успешно отменена
		if resp.Success {

			slog.Warn("Ws получено сообщение об успешной отмене подписке", "exchangeName", exchange.ExchangeName, "wsKey", wsKey)

			//возьмем вебсокет
			ws := exchange.Ws[wsKey]
			ws.Lock()

			//найдем топик по id
			var topic string
			var idxToDelete int
			isFound := false
			for i := range ws.CmdBuffer {
				if ws.CmdBuffer[i].ID == resp.ReqID {
					topic = ws.CmdBuffer[i].Topic
					idxToDelete = i
					isFound = true
				}
			}
			if isFound {
				// найдем индекс топика в списке топиков
				if idx := slices.Index(ws.TopicList, topic); idx != -1 {
					// Удаляем топик по найденному индексу
					ws.TopicList = slices.Delete(ws.TopicList, idx, idx+1)
				}

				//удалим команду из списка команд
				ws.CmdBuffer = slices.Delete(ws.CmdBuffer, idxToDelete, idxToDelete+1)
			}

			ws.Unlock()

			return
		}
	}

	// {\"type\":\"snapshot\",\"topic\":\"kline.15.BTCUSDT\",\"data\":[{\"start\":1793748600000,\"end\":1793749499999,\"interval\":\"15\",\"open\":\"77919\",\"close\":\"77804.3\",\"high\":\"77993.1\",\"low\":\"77804.3\",\"volume\":\"91.622619\",\"turnover\":\"7137154.9407247\",\"confirm\":false,\"timestamp\":1793749427563}],\"ts\":1793749427563}"

	//разделим топик
	topicSlice := strings.Split(resp.Topic, ".")
	//если длин больше 1 в топике что то лежит, потому как Slice от "" дает длину 1
	if len(topicSlice) > 1 {

		//если нулевой элемент kline
		if topicSlice[0] == "kline" {

			//распарсим data
			var klines []WsBybitKlineItem
			if err := json.Unmarshal(resp.Data, &klines); err != nil {
				slog.Error("Ошибка парсинга последней свечи Kline", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "topic", resp.Topic, "err", err)
				return
			}

			//сформируем ключ для поиска записи kline
			key := fmt.Sprintf("%s:%s", wsKey, resp.Topic)

			//Блокируем общее хранилище на чтение
			exchange.AllKlineData.RLock()

			// разблокировать ПРИ ЛЮБОМ ВЫХОДЕ из функции
			defer exchange.AllKlineData.RUnlock()

			//ищем карту
			klineDataMu, ok := exchange.AllKlineData.KlineDataMap[key]
			if !ok {
				slog.Error("Ошибка поиска записи kline", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "topic", resp.Topic, "key", key)
				return // Теперь тут RUnlock() вызовется автоматически благодаря defer! Никакого дедлока!
			}

			klineDataMu.Lock()
			// Изменяем последнюю свечку TOHLC
			klineDataMu.KlineData.LastCandle = []string{
				strconv.FormatInt(klines[0].Start, 10),
				klines[0].Open,
				klines[0].High,
				klines[0].Low,
				klines[0].Close,
			}
			//отметим время измененея
			klineDataMu.KlineData.Ts = time.Now()
			klineDataMu.Unlock()

			return
		}
	}

	//если это какоето другое сообщение
	// if isPong == false {
	// slog.Warn("Ws пришло сообщение", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "message", string(message))

	// //ответ на операцию subscribe/unsubscribe и другие
	// var opResp WsBybitOpResponseStatus
	// if err := json.Unmarshal(message, &opResp); err != nil {
	// 	slog.Error("Ws ошибка базового парсинга", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "err", err)
	// 	return
	// }
	// switch {
	// case strings.HasPrefix(opResp.Topic, "publicTrade"):
	// 	//если получены сделки
	// 	var trades []WsBybitTradeItemShort
	// 	if err := json.Unmarshal(opResp.Data, &trades); err != nil {
	// 		slog.Error("Ошибка парсинга сделок", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "topic", baseResp.Topic, "err", err)

	// 	}
	// 	// Логика обработки сде
	// }

	//ответ с данными
	// var baseResp WsBybitDataResponse
	// if err := json.Unmarshal(message, &baseResp); err != nil {
	// 	slog.Error("Ws ошибка базового парсинга", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "err", err)
	// 	return
	// }

	// // Запускаем проверку топика. Используем strings.HasPrefix,
	// // так как в конце топика всегда идет имя монеты (например, .BTCUSDT)
	// switch {
	// case strings.HasPrefix(baseResp.Topic, "publicTrade"):
	// 	//если получены сделки
	// 	var trades []WsBybitTradeItemShort
	// 	if err := json.Unmarshal(baseResp.Data, &trades); err != nil {
	// 		slog.Error("Ошибка парсинга сделок", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "topic", baseResp.Topic, "err", err)

	// 	}
	// 	// Логика обработки сделок
	// 	slog.Info("Получены сделки", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "topic", baseResp.Topic, "trades", trades)

	// case strings.HasPrefix(baseResp.Topic, "kline"):
	// 	//если получена последняя свеча kline

	// 	// slog.Warn("Ws пришло сообщение", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "message", string(message))

	// 	var klines []WsBybitKlineItem
	// 	if err := json.Unmarshal(baseResp.Data, &klines); err != nil {
	// 		slog.Error("Ошибка парсинга последней свечи Kline", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "topic", baseResp.Topic, "err", err)

	// 	}

	// 	// Логика обработки последних свечей
	// 	slog.Info("Получена последняя свечка kline", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "topic", baseResp.Topic, "klines", klines)

	// 	return
	// }

	slog.Warn("Ws сообщение не обработано", "exchangeName", exchange.ExchangeName, "wsKey", wsKey, "message", string(message))

	// }
	// }
}

// подписывается на канал получения последней свечи
func (b *BybitClient) WsSubscribeLastCandle(exchange *shared.Exchange, contract string, category string, interval string) {

	//конвертируем интервал во внутренний
	intervalConverted, err := b.ConvertInterval(interval)
	if err != nil {
		slog.Error("Ошибка не удалось конвертировать интервал")
		return
	}

	//получим вебсокет
	var ws *shared.WsData
	var ok bool
	switch category {
	case "spot":
		//получим ws спота
		ws, ok = exchange.Ws["spot"]
		if !ok {
			slog.Error("Не удалось найти ws spot")
			return
		}

	case "perpetual", "futures":
		//получим ws спота
		ws, ok = exchange.Ws["linear"]
		if !ok {
			slog.Error("Не удалось найти ws linear")
			return
		}

	default:
		slog.Error("Ошибка в запросе: такой категории не существует")
		return
	}

	//получим текущий список топиков для вебсокета
	ws.Lock()
	topicListCopy := make([]string, len(ws.TopicList))
	copy(topicListCopy, ws.TopicList)
	ws.Unlock()

	//сформируем топик
	klineTopik := fmt.Sprintf("kline.%s.%s", intervalConverted, contract)

	// если категория не содержит запись о подписке
	if !slices.Contains(topicListCopy, klineTopik) {
		slog.Warn("Топик не открыт")

		if ws.CmdChan == nil {
			slog.Warn("Канал для управления ws не определен")
			return
		}

		// составим сообщение
		cmdMsg := shared.CmdChanMsg{
			ID:         exchange.GenID.Next(),
			Action:     "subscribe",
			Topic:      klineTopik,
			DeleteTime: time.Now().Add(exchange.FetchIntervals.WsCmdMsgDeleteDelta),
		}

		// Отправляем сообщение в канал управления ws
		ws.CmdChan <- cmdMsg

	}
}

func (b *BybitClient) MakeKlineKey(contract string, category string, interval string) (string, error) {

	intervalConverted, err := b.ConvertInterval(interval)
	if err != nil {
		return "", fmt.Errorf("Ошибка конвертации интервала: %s", err)
	}

	switch category {
	case "spot":
		return fmt.Sprintf("spot:kline.%s.%s", intervalConverted, contract), nil
	case "perpetual", "futures":
		return fmt.Sprintf("linear:kline.%s.%s", intervalConverted, contract), nil
	default:
		return "", fmt.Errorf("Ошибка конвертации интервала: нет такой категории %s", category)
	}

}

//------- ВСПОМОГАТЕЛЬНЫЕ УНИКАЛЬНЫЕ ФУНКЦИИ КЛИЕНТА ------

// локальная функция декодирует ответ понг сервера
// func WsDecodePong(message []byte) (bool, error) {

// 	var resp WsPongResponse

// 	// Десериализуем JSON. Если это рыночные данные другого формата,
// 	// unmarshal может выдать ошибку или просто оставить поля пустыми.
// 	if err := json.Unmarshal(message, &resp); err != nil {
// 		return false, err
// 	}

// 	// Проверяем, совпадает ли хотя бы одно поле с признаком pong
// 	if resp.RetMsg == "pong" {
// 		return true, nil
// 	}

// 	return false, nil
// }

// вытаскивает из сообщения значение lastPrice
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
