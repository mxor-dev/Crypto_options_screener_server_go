package shared

import (
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

// биржа
type Exchange struct {
	IsActive       bool
	ExchangeName   string
	AllSymbols     map[string]*SymbolData
	FetchIntervals FetchIntervals
	KlineLimit     string
	Client         Client

	AllKlineData KlineDataMapMu

	Ws map[string]*WsData

	GenID *GenIDMu
}

// символы
type SymbolData struct {
	Symbol      string
	FetchFilter []string

	Info InfoMu

	TikersData TikersDataMu
}

type InfoMu struct {
	SymbolInfo SymbolInfo
	sync.Mutex
}

// функция для сохранения данных
func (i *InfoMu) SaveInfo(symbolInfo SymbolInfo) {
	i.Lock()
	i.SymbolInfo = symbolInfo
	i.Unlock()
}

// функция для получения данных
func (i *InfoMu) GetInfo() SymbolInfo {
	i.Lock()
	result := i.SymbolInfo
	i.Unlock()

	return result
}

type SymbolInfo struct {
	ExchangeName string `json:"exchange_name"`
	Symbol       string `json:"symbol"`

	SpotContractName   string `json:"spot_contract_name"`
	SpotMinOrderQty    string `json:"spot_min_order_qty"`
	SpotTickSize       string `json:"spot_tick_size"`
	SpotPricePrecision string `json:"spot_price_precision"`

	PerpetualContractName   string `json:"perpetual_contract_name"`
	PerpetualMinOrderQty    string `json:"perpetual_min_order_qty"`
	PerpetualTickSize       string `json:"perpetual_tick_size"`
	PerpetualPricePrecision string `json:"perpetual_price_precision"`

	FuturesContractNameArr []string `json:"futures_contract_name_arr"`
	FuturesMinOrderQty     string   `json:"futures_min_order_qty"`
	FuturesTickSize        string   `json:"futures_tick_size"`
	FuturesPricePrecision  string   `json:"futures_price_precision"`

	OptionsMinOrderQty    string `json:"options_min_order_qty"`
	OptionsTickSize       string `json:"options_tick_size"`
	OptionsPricePrecision string `json:"options_price_precision"`

	IsSpotAvailable      bool `json:"is_spot_available"`
	IsPerpetualAvailable bool `json:"is_perpetual_available"`
	IsFuturesAvailable   bool `json:"is_futures_available"`
	IsOptionsAvailable   bool `json:"is_options_available"`
}

type KlineDataMapMu struct {
	KlineDataMap map[string]*KlineDataMu
	sync.RWMutex
}

type KlineDataMu struct {
	KlineData KlineData
	sync.Mutex
}

type KlineData struct {
	Symbol   string
	Contract string
	Category string
	Interval string

	Klines     [][]string //массив формат TOHLC
	LastCandle []string   //одна свечка формат TOHLC

	Ts time.Time

	DeleteTime time.Time
}

func (a *KlineDataMapMu) CreateKlineData(contract string, category string, interval string) {
	a.Lock()

	//составной ключ
	key := fmt.Sprintf("%s:%s:%s", contract, category, interval)

	//поищем запись в хеш таблице
	_, ok := a.KlineDataMap[key]

	//если записи не существует
	if !ok {
		//добавим запиись
		a.KlineDataMap[key] = &KlineDataMu{}
		slog.Info("Запись добавлена KlineData", "contract", contract, "category", category, "interval", interval)
	} else {
		//если зипись сущетвует
		slog.Error("Ошибка при добавлении KlineData: запись уже существует.", "contract", contract, "category", category, "interval", interval)
	}
	a.Unlock()
}
func (a *KlineDataMapMu) DeleteKlineData(contract string, category string, interval string) {
	a.Lock()

	//составной ключ
	key := fmt.Sprintf("%s:%s:%s", contract, category, interval)

	//поищем запись в хеш таблице
	_, ok := a.KlineDataMap[key]

	//если записи не существует
	if !ok {
		//удалим запись
		delete(a.KlineDataMap, key)
		slog.Info("Запись удалена KlineData", "contract", contract, "category", category, "interval", interval)
	} else {
		//если зипись сущетвует
		slog.Error("Ошибка при удалении KlineData: запись не существует.", "contract", contract, "category", category, "interval", interval)
	}
	a.Unlock()
}
func (a *KlineDataMapMu) SetDeleteTime(contract string, category string, interval string, deleteTime time.Time) {
	a.Lock()

	//составной ключ
	key := fmt.Sprintf("%s:%s:%s", contract, category, interval)

	//поищем запись в хеш таблице
	item, ok := a.KlineDataMap[key]

	//если записи не существует
	if !ok {
		//установим время удления
		item.Lock()
		item.KlineData.DeleteTime = deleteTime
		item.Unlock()
		slog.Info("Запись удалена KlineData", "contract", contract, "category", category, "interval", interval)
	}
	a.Unlock()
}

// функция для сохранения данных kline
func (k *KlineDataMu) SaveKlines(klines [][]string) {
	//запишем данные kline
	k.Lock()
	//если длина не нулевая
	if len(klines) > 0 {
		k.KlineData.Klines = klines
		k.KlineData.LastCandle = klines[len(klines)-1]
	}
	k.Unlock()

	// slog.Warn("Двнные KlineData сохранены", "klineData", klineData)
}

// функция для получения данных kline
func (k *KlineDataMu) GetKlineData() KlineData {

	//запишем данные kline
	k.Lock()
	result := k.KlineData
	k.Unlock()

	// slog.Warn("Двнные KlineData сохранены", "klineData", klineData)
	return result
}

// функция для получения последней свечи
func (k *KlineDataMu) GetKlineLastCandle() []string {

	k.Lock()
	//возьмем последнюю свечку
	result := k.KlineData.LastCandle
	k.Unlock()

	return result
}

// функция для обновления свечи
func (k *KlineDataMu) UpdateKlineLastCandle(lastPrice string) {

	k.Lock()

	//выделим нужное из свечки TOHLC
	high := k.KlineData.LastCandle[2]
	low := k.KlineData.LastCandle[3]
	close := k.KlineData.LastCandle[4]

	// slog.Warn("Текущая lastCandle", "klineData", k)

	//конвертикуем последнюю цену в число
	lastPriceFloat, err := strconv.ParseFloat(close, 64)
	if err != nil {
		slog.Error("Ошибка конвертации строки во float64", "func", "UpdateKlineLastCandle", "error", err)
	}
	//конвертикуем текущий хай в число
	currHightFloat, err := strconv.ParseFloat(high, 64)
	if err != nil {
		slog.Error("Ошибка конвертации строки во float64", "func", "UpdateKlineLastCandle", "error", err)
	}
	//конвертикуем текущий лоу в число
	cyrrLowFloat, err := strconv.ParseFloat(low, 64)
	if err != nil {
		slog.Error("Ошибка конвертации строки во float64", "func", "UpdateKlineLastCandle", "error", err)
	}

	//если последняя цена больше текущего хай
	if lastPriceFloat > currHightFloat {
		//заменим текущий хай
		k.KlineData.LastCandle[2] = lastPrice
	}

	//если последняя цена меньше текущего лоу
	if lastPriceFloat > cyrrLowFloat {
		//заменим текущий лоу
		k.KlineData.LastCandle[3] = lastPrice
	}

	//обновим цену закрытия
	k.KlineData.LastCandle[4] = lastPrice

	// slog.Warn("Обновленная lastCandle", "klineData", k)

	k.Unlock()

}

// функция для получения последней цены закрытия
func (k *KlineDataMu) GetKlineLastCandleClosePrice() string {

	//по умолчанию запишем нуль
	result := "0"

	k.Lock()
	//если длина данных klines не нулевая
	if len(k.KlineData.Klines) != 0 {
		result = k.KlineData.Klines[len(k.KlineData.Klines)-1][4] //свечка в формате TOHLC
	}
	k.Unlock()

	return result
}

// интервалы запроса данных
type FetchIntervals struct {
	RestDataGetInterval    time.Duration
	LinearDataGetInterval  time.Duration
	OptionsDataGetInterval time.Duration

	KlineDeleteDeltaTime time.Duration

	WsDeadLineDelta     time.Duration
	WsCmdMsgDeleteDelta time.Duration
}

// type SymbolDataToDb struct {
// 	ExchangeName    string
// 	Symbol          string
// 	LastPriceSpot   string
// 	LastPriceLinear string
// 	LinearData      []LinearData
// 	OptionsData     []OptionsData
// 	TimeMs          string
// }

type TikersDataMu struct {
	Spot      TickersDataSpot
	Perpetual TickersDataPerpetual
	Futures   []TickersDataFutures
	Options   []TickersDataOptions
	sync.Mutex
}

type TickersDataSpot struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bid_price"`
	BidSize  string `json:"bid_size"`
	AskPrice string `json:"ask_price"`
	AskSize  string `json:"ask_size"`
}

type TickersDataPerpetual struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bid_price"`
	BidSize  string `json:"bid_size"`
	AskPrice string `json:"ask_price"`
	AskSize  string `json:"ask_size"`
}

type TickersDataFutures struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bid_price"`
	BidSize  string `json:"bid_size"`
	AskPrice string `json:"ask_price"`
	AskSize  string `json:"ask_size"`
}

type TickersDataOptions struct {
	Symbol       string       `json:"symbol"`
	Series       string       `json:"series"`
	ExpiryTime   string       `json:"expiry_time"`
	MinMarkIv    string       `json:"min_mark_iv"`
	OptionsLines OptionsLines `json:"options_lines"`
}

type OptionsLines struct {
	Strikes []string `json:"strikes"`

	Cmark []string `json:"c_mark"`
	Pmark []string `json:"p_mark"`
	Cask  []string `json:"c_ask"`
	Pask  []string `json:"p_ask"`
	Cbid  []string `json:"c_bid"`
	Pbid  []string `json:"p_bid"`

	Cdelta []string `json:"c_delta"`
	Pdelta []string `json:"p_delta"`
	Cgamma []string `json:"c_gamma"`
	Pgamma []string `json:"p_gamma"`
	Cvega  []string `json:"c_vega"`
	Pvega  []string `json:"p_vega"`
	Ctheta []string `json:"c_theta"`
	Ptheta []string `json:"p_theta"`

	Coi   []string `json:"c_oi"`
	Poi   []string `json:"p_oi"`
	Ctv   []string `json:"c_tv"`
	Ptv   []string `json:"p_tv"`
	Cv24h []string `json:"c_v24h"`
	Pv24h []string `json:"p_v24h"`

	CaskIv []string `json:"c_ask_iv"`
	PaskIv []string `json:"p_ask_iv"`
	CbidIv []string `json:"c_bid_iv"`
	PbidIv []string `json:"p_bid_iv"`
	MarkIv []string `json:"mark_iv"`

	Cgex   []string `json:"c_gex"`
	Pgex   []string `json:"p_gex"`
	Netgex []string `json:"net_gex"`
}

type LastPriceResponse struct {
	Symbol    string
	LastPrice string
}

type CmdChanMsg struct {
	ID         string
	Action     string
	Topic      string
	DeleteTime time.Time
}

type WsData struct {
	CmdChan   chan CmdChanMsg
	TopicList []string
	CmdBuffer []CmdChanMsg
	sync.Mutex
}
type WsTopicListMu struct {
	WsTopicListSpot   []string
	WsTopicListLinear []string
	sync.RWMutex
}

type GenIDMu struct {
	mu      sync.Mutex
	counter int
}

func (g *GenIDMu) Next() string {
	g.mu.Lock()
	g.counter++ // Увеличиваем
	val := g.counter
	g.mu.Unlock()

	return strconv.Itoa(val) // Быстро переводим в строку
}
