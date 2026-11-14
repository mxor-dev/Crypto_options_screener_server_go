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
	FeeRates       FeeRates
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
	SpotContractSize   string `json:"spot_contract_size"`
	SpotMinOrderQty    string `json:"spot_min_order_qty"`
	SpotQtyPrecision   string `json:"spot_qty_precision"`
	SpotTickSize       string `json:"spot_tick_size"`
	SpotPricePrecision string `json:"spot_price_precision"`

	PerpetualContractName   string `json:"perpetual_contract_name"`
	PerpetualContractSize   string `json:"perpetual_contract_size"`
	PerpetualMinOrderQty    string `json:"perpetual_min_order_qty"`
	PerpetualQtyPrecision   string `json:"perpetual_qty_precision"`
	PerpetualTickSize       string `json:"perpetual_tick_size"`
	PerpetualPricePrecision string `json:"perpetual_price_precision"`

	FuturesContractNameArr []string `json:"futures_contract_name_arr"`
	FuturesContractSize    string   `json:"futures_contract_size"`
	FuturesMinOrderQty     string   `json:"futures_min_order_qty"`
	FuturesQtyPrecision    string   `json:"futures_qty_precision"`
	FuturesTickSize        string   `json:"futures_tick_size"`
	FuturesPricePrecision  string   `json:"futures_price_precision"`

	OptionsContractSize   string `json:"options_contract_size"`
	OptionsMinOrderQty    string `json:"options_min_order_qty"`
	OptionsQtyPrecision   string `json:"options_qty_precision"`
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

	Klines [][]string //массив формат TOHLC

	LastCandle []string //одна свечка формат TOHLC

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

// функция для получения данных kline
func (k *KlineDataMu) GetKlineData() KlineData {

	//запишем данные kline
	k.Lock()
	result := k.KlineData
	k.Unlock()

	// slog.Warn("Двнные KlineData сохранены", "klineData", klineData)
	return result
}

// интервалы запроса данных
type FetchIntervals struct {
	RestDataGetInterval    time.Duration
	TickersDataGetInterval time.Duration

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

type FeeRatesSpot struct {
	MakerFeeRate float64 `json:"maker_fee_rate"`
	TakerFeeRate float64 `json:"taker_fee_rate"`
}
type FeeRatesFutures struct {
	MakerFeeRate float64 `json:"maker_fee_rate"`
	TakerFeeRate float64 `json:"taker_fee_rate"`
}

type FeeRatesOptions struct {
	MaxPartRate        float64 `json:"max_part_rate"`
	LiquidationFeeRate float64 `json:"liquidation_fee_rate"`
	MakerFeeRate       float64 `json:"maker_fee_rate"`
	TakerFeeRate       float64 `json:"taker_fee_rate"`
	ExpirationFeeRate  float64 `json:"expiration_fee_rate"`
}

type FeeRates struct {
	Spot    FeeRatesSpot    `json:"spot_fee_rates"`
	Futures FeeRatesFutures `json:"futures_fee_rates"`
	Options FeeRatesOptions `json:"options_fee_rates"`
}
