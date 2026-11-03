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

		OptionContractNameArr: []string{},
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
		for i := range dataOption.Result.List {
			//запишем имя контракта
			result.OptionContractNameArr = append(result.OptionContractNameArr, dataOption.Result.List[i].Symbol)
		}
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
func (b *BybitClient) GetOptions(symbol string) ([]shared.OptionsData, error) {

	result := []shared.OptionsData{}

	//сформируем строку запроса для линейных инструментов
	url := fmt.Sprintf("%s/v5/market/tickers?baseCoin=%s&category=%s", baseEndPont, symbol, "option")

	//выполним запрос
	respOptions, err := http.Get(url)
	if err != nil {
		return []shared.OptionsData{}, fmt.Errorf("Ошбика Get запроса %s: %w", url, err)
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
		return []shared.OptionsData{}, fmt.Errorf("Ошибка чтения тела ответа %s: %w", url, err)
	}

	//экземпляр структуры ответа
	var rawOptionsData OptionsResponse

	//распаковываем JSON-байты в созданный объект Go (передаем по указателю &)
	err = json.Unmarshal(bodyBytes, &rawOptionsData)
	if err != nil {
		return []shared.OptionsData{}, fmt.Errorf("Ошибка парсинга JSON опционов: %w", err)
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
	for i := range rawOptionsData.Result.List {

		//разобъем Symbol на части
		parts := strings.Split(rawOptionsData.Result.List[i].Symbol, "-")

		//переменные
		currSeries := parts[1]
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
				Data:   rawOptionsData.Result.List[i],
			}

			//запишем данные опциона в Calls
			currSeriesData.Calls = append(currSeriesData.Calls, localData)
		case "P":
			// slog.Warn("Добавлен Put")

			//создадим локальную структуру опциона
			localData := StrikeAndData{
				Strike: strike,
				Data:   rawOptionsData.Result.List[i],
			}

			////запишем данные опциона в Puts
			currSeriesData.Puts = append(currSeriesData.Puts, localData)
		}

		//записываем обновленную серию обратно в map
		allSeriesMap[currSeries] = currSeriesData

		// slog.Warn("Получены данные по опционам", "ExchangeName", b.ExchangeName, "symbol", symbol, "currSeriesData", currSeriesData)

	}

	// slog.Warn("Получены данные по опционам", "ExchangeName", b.ExchangeName, "symbol", symbol, "allSeriesMap", allSeriesMap)

	//для каждой серии
	for series, seriesData := range allSeriesMap {

		//массив трайков
		var allStrikes []string

		//соберем все страйки их опционов Call
		for _, call := range seriesData.Calls {
			allStrikes = append(allStrikes, call.Strike)
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
		}

		//передадим страйки
		copy(localOptionsLines.Strikes, allStrikes)

		//для каждого опциона call
		for _, call := range seriesData.Calls {

			//индекс страйка
			var strikeIndex int

			//для каждого страйка в данных по страйкам
			for i, strike := range localOptionsLines.Strikes {
				//если страйки совпадают
				if call.Strike == strike {
					//запишем индекс страйка
					strikeIndex = i
					break
				}
			}

			//перепишем данные Call опциона в соответствии с индексом
			localOptionsLines.Cmark[strikeIndex] = call.Data.MarkPrice
			localOptionsLines.Cask[strikeIndex] = call.Data.Ask1Price
			localOptionsLines.Cbid[strikeIndex] = call.Data.Bid1Price

			localOptionsLines.Cdelta[strikeIndex] = call.Data.Delta
			localOptionsLines.Cgamma[strikeIndex] = call.Data.Gamma
			localOptionsLines.Cvega[strikeIndex] = call.Data.Vega
			localOptionsLines.Ctheta[strikeIndex] = call.Data.Theta

			localOptionsLines.Coi[strikeIndex] = call.Data.OpenInterest
			localOptionsLines.Ctv[strikeIndex] = call.Data.TotalVolume
			localOptionsLines.Cv24h[strikeIndex] = call.Data.Volume24h

			localOptionsLines.CaskIv[strikeIndex] = call.Data.Ask1Iv
			localOptionsLines.CbidIv[strikeIndex] = call.Data.Bid1Iv

			//передадим линию MarkIv
			localOptionsLines.MarkIv[strikeIndex] = call.Data.MarkIv

		}

		//для каждого опциона put
		for _, put := range seriesData.Puts {

			//индекс страйка
			var strikeIndex int

			//для каждого страйка в данных по страйкам
			for i, strike := range localOptionsLines.Strikes {
				//если страйки совпадают
				if put.Strike == strike {
					//запишем индекс страйка
					strikeIndex = i
					break
				}
			}

			//перепишем данные Put опциона в соответствии с индексом
			localOptionsLines.Pmark[strikeIndex] = put.Data.MarkPrice
			localOptionsLines.Pask[strikeIndex] = put.Data.Ask1Price
			localOptionsLines.Pbid[strikeIndex] = put.Data.Bid1Price

			localOptionsLines.Pdelta[strikeIndex] = put.Data.Delta
			localOptionsLines.Pgamma[strikeIndex] = put.Data.Gamma
			localOptionsLines.Pvega[strikeIndex] = put.Data.Vega
			localOptionsLines.Ptheta[strikeIndex] = put.Data.Theta

			localOptionsLines.Poi[strikeIndex] = put.Data.OpenInterest
			localOptionsLines.Ptv[strikeIndex] = put.Data.TotalVolume
			localOptionsLines.Pv24h[strikeIndex] = put.Data.Volume24h

			localOptionsLines.PaskIv[strikeIndex] = put.Data.Ask1Iv
			localOptionsLines.PbidIv[strikeIndex] = put.Data.Bid1Iv

		}

		// slog.Warn("Получены страйки", "ExchangeName", b.ExchangeName, "series", series, "localOptionsLines", localOptionsLines)

		//минимум линии
		var minMarkIvStr string

		//если линия MarkIv не нулевая
		if len(localOptionsLines.MarkIv) > 0 {

			//первое значение макс float64
			minMarkIv := math.MaxFloat64

			// slog.Warn("Debug", "ExchangeName", b.ExchangeName, "symbol", symbol, "localOptionsLines.MarkIv", localOptionsLines.MarkIv)

			for _, markIvStr := range localOptionsLines.MarkIv {

				// slog.Warn("Debug", "ExchangeName", b.ExchangeName, "symbol", symbol, "markIvStr", markIvStr)

				// Конвертируем строку в число (база 10, размер int)
				num, err := strconv.ParseFloat(markIvStr, 64)
				if err != nil {
					// Если в массиве попалась не строка-число (например "abc"), возвращаем ошибку
					return []shared.OptionsData{}, fmt.Errorf("Ошибка конвертации строки '%s' в число: %w", markIvStr, err)
				}

				// Если текущее число меньше сохраненного минимума, обновляем минимум
				if num < minMarkIv {
					//сохраним числовое значение
					minMarkIv = num
					//сохраним строковое значение
					minMarkIvStr = markIvStr
				}
			}
		}

		//заполним данные для текущей серии опционов
		localOptionsData := shared.OptionsData{
			Symbol:       symbol,
			Series:       series,
			MinMarkIv:    minMarkIvStr,
			OptionsLines: localOptionsLines,
		}

		//добавим полученные данные по серии в итоговый массив
		result = append(result, localOptionsData)
	}

	//сортируем итоговый результат по полю series по возрастанию времени доставки
	slices.SortFunc(result, func(a, b shared.OptionsData) int {
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

	slog.Info("Получены данные Option", "ExchangeName", b.ExchangeName, "symbol", symbol, "len(result)", len(result))

	return result, nil
}

// вспомогательный воркер для отправики сообщений ping
func (b *BybitClient) WsPingTimer(ctx context.Context, conn *websocket.Conn, stopChan chan struct{}, exchangeName string, baseEndPoint string) {

	slog.Info("Ws ping запуск воркера", "exchangeName", exchangeName, "baseEndPoint", baseEndPoint)

	// Создаем тикер с интервалом в 20 секунд
	ticker := time.NewTicker(20 * time.Second)
	defer func() {
		ticker.Stop()
		slog.Info("Ws ping таймер остановлен", "exchangeName", exchangeName, "baseEndPoint", baseEndPoint)
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
				slog.Error("Ws ошибка отправки JSON Ping", "exchangeName", exchangeName, "baseEndPoint", baseEndPoint, "error", err)
				conn.Close() // Принудительно закрываем сокет, чтобы разблокировать цикл чтения

				slog.Info("Ws работа вспомогательной горутины ping-pong завершена по отключению вебсокета", "exchangeName", exchangeName, "baseEndPoint", baseEndPoint)
				return // Выходим из горутины, сокет отключен
			}

			slog.Debug("Ws на сервер отправлен запрос ping", "exchangeName", exchangeName, "baseEndPoint", baseEndPoint)
		case <-stopChan:

			slog.Info("Ws ping воркер остановлен по сигналу", "exchangeName", exchangeName, "baseEndPoint", baseEndPoint)
			return // Выходим по внешнемук сигналу

		case <-ctx.Done():
			slog.Info("Ws ping воркер остановлен по контксту", "exchangeName", exchangeName, "baseEndPoint", baseEndPoint)
			return // Выходим по контексту приложения
		}
	}
}

// обработка сообщение и передача данных в общее хранилище
func (b *BybitClient) WsDecodeMessage(exchange *shared.Exchange, baseEndPoint string, conn *websocket.Conn, message []byte) {
	//еcли ответ - пустое ообщение - это переподключение
	if string(message) == "" {
		slog.Debug("Ws переподключен", "ExchangeName", exchange.ExchangeName)
	} else {

		//базовый ответ
		var resp WsBybitBaseResponse
		if err := json.Unmarshal(message, &resp); err != nil {
			slog.Error("Ws ошибка базового парсинга", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "err", err)
			return
		}

		switch resp.RetMsg {
		case "pong":
			// если это сообщение pong

			slog.Debug("Ws от сервера получен ответ pong", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint)

			//передвинем дедлайн
			err := conn.SetReadDeadline(time.Now().Add(exchange.FetchIntervals.WsDeadLineDelta))
			if err != nil {
				slog.Error("Ws ошибка при декодировании pong", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "error", err)
			}

			return

		case "subscribe":
			//если это ответ на сообщение о подписке
			// {\"success\":true,\"ret_msg\":\"subscribe\",\"conn_id\":\"dbhf1usmqckqr6btpve0-120x6\",\"req_id\":\"2\",\"op\":\"subscribe\"}"

			//если подписка успешно оформлена
			if resp.Success {
				//возьмем вебсокет
				ws := exchange.Ws[baseEndPoint]
				ws.Lock()

				//найдем топик по id
				var topic string
				for i := range ws.CmdBuffer {
					if ws.CmdBuffer[i].ID == resp.ReqID {
						topic = ws.CmdBuffer[i].Topic
					}
				}
				//добавим топик
				ws.TopicList = append(ws.TopicList, topic)
				ws.Unlock()

				return
			}

		}

		switch expression {
		case condition:
			***
		}

		{\"type\":\"snapshot\",\"topic\":\"kline.15.BTCUSDT\",\"data\":[{\"start\":1793748600000,\"end\":1793749499999,\"interval\":\"15\",\"open\":\"77919\",\"close\":\"77804.3\",\"high\":\"77993.1\",\"low\":\"77804.3\",\"volume\":\"91.622619\",\"turnover\":\"7137154.9407247\",\"confirm\":false,\"timestamp\":1793749427563}],\"ts\":1793749427563}"
		//если это какоето другое сообщение
		// if isPong == false {
		// slog.Warn("Ws пришло сообщение", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "message", string(message))

		// //ответ на операцию subscribe/unsubscribe и другие
		// var opResp WsBybitOpResponseStatus
		// if err := json.Unmarshal(message, &opResp); err != nil {
		// 	slog.Error("Ws ошибка базового парсинга", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "err", err)
		// 	return
		// }
		// switch {
		// case strings.HasPrefix(opResp.Topic, "publicTrade"):
		// 	//если получены сделки
		// 	var trades []WsBybitTradeItemShort
		// 	if err := json.Unmarshal(opResp.Data, &trades); err != nil {
		// 		slog.Error("Ошибка парсинга сделок", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "topic", baseResp.Topic, "err", err)

		// 	}
		// 	// Логика обработки сде
		// }

		//ответ с данными
		// var baseResp WsBybitDataResponse
		// if err := json.Unmarshal(message, &baseResp); err != nil {
		// 	slog.Error("Ws ошибка базового парсинга", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "err", err)
		// 	return
		// }

		// // Запускаем проверку топика. Используем strings.HasPrefix,
		// // так как в конце топика всегда идет имя монеты (например, .BTCUSDT)
		// switch {
		// case strings.HasPrefix(baseResp.Topic, "publicTrade"):
		// 	//если получены сделки
		// 	var trades []WsBybitTradeItemShort
		// 	if err := json.Unmarshal(baseResp.Data, &trades); err != nil {
		// 		slog.Error("Ошибка парсинга сделок", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "topic", baseResp.Topic, "err", err)

		// 	}
		// 	// Логика обработки сделок
		// 	slog.Info("Получены сделки", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "topic", baseResp.Topic, "trades", trades)

		// case strings.HasPrefix(baseResp.Topic, "kline"):
		// 	//если получена последняя свеча kline

		// 	// slog.Warn("Ws пришло сообщение", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "message", string(message))

		// 	var klines []WsBybitKlineItem
		// 	if err := json.Unmarshal(baseResp.Data, &klines); err != nil {
		// 		slog.Error("Ошибка парсинга последней свечи Kline", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "topic", baseResp.Topic, "err", err)

		// 	}

		// 	// Логика обработки последних свечей
		// 	slog.Info("Получена последняя свечка kline", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "topic", baseResp.Topic, "klines", klines)

		// 	return
		// }

		slog.Warn("Ws сообщение не обработано", "exchangeName", exchange.ExchangeName, "baseEndPoint", baseEndPoint, "message", string(message))

		// }
	}
}

// формирует топик для получения потока сделок
// func (b *BybitClient) WsFormTopicGetTrades(cmdMsg shared.CmdChanMsg) (string, error) {
// 	//получим аргументы сообщения - это массив строк, первая из которых в данном случае должен быть контракт
// 	args := cmdMsg.Args

// 	if len(args) == 0 {
// 		return "", fmt.Errorf("Ошибка WsFormTopicGetTrades: len(args) ==0")
// 	}

// 	topic := fmt.Sprintf("publicTrade.%s", args[0])
// 	return topic, nil

// }

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
