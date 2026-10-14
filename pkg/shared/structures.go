package shared

import (
	"sync"
	"time"
)

// биржа
type Exchange struct {
	IsActive  bool
	Name      string
	Symbols   []Symbols
	Intervals Intervals
	Client    Client
}

// символы
type Symbols struct {
	Symbol     string
	Categories []string

	Info InfoMu

	KlineSpot   KlineDataMu
	KlineLinear KlineDataMu

	LastCandleSpot   KlineLastCandleMu
	LastCandleLinear KlineLastCandleMu
}

// интервалы запроса данных
type Intervals struct {
	RestDataGetInterval    time.Duration
	LinearDataGetInterval  time.Duration
	OptionsDataGetInterval time.Duration
}
type KlineDataMu struct {
	KlineData KlineData
	sync.Mutex
}

// функция для сохранения данных
func (k *KlineDataMu) SaveKlineData(klineData KlineData) {
	//запишем данные kline
	k.Lock()
	k.KlineData = klineData
	k.Unlock()
}

type KlineData struct {
	ExchangeName string
	Symbol       string

	Category      string
	KlineInterval string

	KlineData [][]string //формат TOHLC
	Time      int64
}

type KlineLastCandleMu struct {
	Time  string
	Open  string
	Hight string
	Low   string
	Close string

	sync.Mutex
}

// функция для сохранения данных
func (k *KlineLastCandleMu) SaveKlineLastCandle(lastCandle []string) {
	//запишем данные kline
	k.Lock()
	k.Time = lastCandle[0]
	k.Open = lastCandle[1]
	k.Hight = lastCandle[2]
	k.Low = lastCandle[3]
	k.Close = lastCandle[4]
	k.Unlock()
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

type SymbolInfo struct {
	ExchangeName string
	Symbol       string

	SpotMinOrderQty    string
	SpotTickSize       string
	SpotPricePrecision string

	LinearMinOrderQty    string
	LinearTickSize       string
	LinearPricePrecision string

	OptionMinOrderQty    string
	OptionTickSize       string
	OptionPricePrecision string
}

type LastPriceResponse struct {
	Symbol    string
	LastPrice string
}
