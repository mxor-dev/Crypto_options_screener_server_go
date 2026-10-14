package shared

import (
	"log/slog"
	"strconv"
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

	// slog.Warn("Двнные KlineData сохранены", "klineData", klineData)
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

	// slog.Warn("Данные lastCandle сохранены", "lastCandle", lastCandle)
}

// функция для получения данных
func (k *KlineLastCandleMu) GetKlineLastCandle() []string {

	var result []string

	//запишем данные kline
	k.Lock()
	result = append(result, k.Time)
	result = append(result, k.Open)
	result = append(result, k.Hight)
	result = append(result, k.Low)
	result = append(result, k.Close)
	k.Unlock()

	return result
}

// функция для обновления свечи
func (k *KlineLastCandleMu) UpdateKlineLastCandle(lastPrice string) {

	k.Lock()

	// slog.Warn("Текущая lastCandle", "klineData", k)

	//конвертикуем последнюю цену в число
	lastPriceFloat, err := strconv.ParseFloat(lastPrice, 64)
	if err != nil {
		slog.Error("Ошибка конвертации строки во float64", "UpdateKlineLastCandle", "error", err)
	}
	//конвертикуем текйщий лоу в число
	currHightFloat, err := strconv.ParseFloat(k.Hight, 64)
	if err != nil {
		slog.Error("Ошибка конвертации строки во float64", "UpdateKlineLastCandle", "error", err)
	}
	//конвертикуем текущий лоу в число
	cyrrLowFloat, err := strconv.ParseFloat(k.Low, 64)
	if err != nil {
		slog.Error("Ошибка конвертации строки во float64", "UpdateKlineLastCandle", "error", err)
	}

	//если последняя цена больше текущего хай
	if lastPriceFloat > currHightFloat {
		//заменим текущий хай
		k.Hight = lastPrice
	}

	//если последняя цена меньше текущего лоу
	if lastPriceFloat > cyrrLowFloat {
		//заменим текущий лоу
		k.Low = lastPrice
	}

	//обновим цену закрытия
	k.Close = lastPrice

	// slog.Warn("Обновленная lastCandle", "klineData", k)

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
