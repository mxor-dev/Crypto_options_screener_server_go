package shared

import (
	"log/slog"
	"strconv"
	"sync"
	"time"
)

// биржа
type Exchange struct {
	IsActive       bool
	ExchangeName   string
	AllSymbols     map[string]*AllSymbols
	FetchIntervals FetchIntervals
	Client         Client
}

// символы
type AllSymbols struct {
	Symbol      string
	FetchFilter []string

	Info InfoMu

	KlineSpot   map[string]*KlineDataMu
	KlineLinear map[string]*KlineDataMu

	OptionsData []OptionsData
}

// интервалы запроса данных
type FetchIntervals struct {
	RestDataGetInterval    time.Duration
	LinearDataGetInterval  time.Duration
	OptionsDataGetInterval time.Duration
}

type KlineData struct {
	ExchangeName string
	Symbol       string
	Category     string

	KlineInterval string

	KlineData  [][]string //массив формат TOHLC
	LastCandle []string   //одна свечка формат TOHLC

	Time int64
}

type KlineDataMu struct {
	KlineData KlineData
	sync.Mutex
}

// функция для сохранения данных kline
func (k *KlineDataMu) SaveKlineData(klineData KlineData) {
	//запишем данные kline
	k.Lock()
	k.KlineData = klineData
	k.Unlock()

	// slog.Warn("Двнные KlineData сохранены", "klineData", klineData)
}

// функция для получения данных kline
func (k *KlineDataMu) LoadKlineData() KlineData {

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
	result := k.KlineData.KlineData[len(k.KlineData.KlineData)-1]
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

	k.Lock()
	result := k.KlineData.KlineData[len(k.KlineData.KlineData)-1][4] //свечка в формате TOHLC
	k.Unlock()

	return result
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

type OptionsData struct {
	Symbol       string
	Series       string
	MinMarkIv    string
	OptionsLines OptionsLines
}

type OptionsLines struct {
	Strikes []string

	Cmark []string
	Pmark []string
	Cask  []string
	Pask  []string
	Cbid  []string
	Pbid  []string

	Cdelta []string
	Pdelta []string
	Cgamma []string
	Pgamma []string
	Cvega  []string
	Pvega  []string
	Ctheta []string
	Ptheta []string

	Coi   []string
	Poi   []string
	Ctv   []string
	Ptv   []string
	Cv24h []string
	Pv24h []string

	CaskIv []string
	PaskIv []string
	CbidIv []string
	PbidIv []string
	MarkIv []string
}

type SymbolData struct {
	ExchangeName    string
	Symbol          string
	LastPriceSpot   string
	LastPriceLinear string
	LinearData      []LinearData
	OptionsData     []OptionsData
	TimeMs          string
}

type LinearData struct {
}
