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
	Client         Client

	Ws map[string]*WsData

	GenID *GenIDMu
}

// символы
type SymbolData struct {
	Symbol      string
	FetchFilter []string

	Info InfoMu

	AllKlineData KlineDataMapMu

	OptionsData []OptionsData //протом проработать в mutex
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

	SpotContractName   string
	SpotMinOrderQty    string
	SpotTickSize       string
	SpotPricePrecision string

	PerpetualContractName   string
	PerpetualMinOrderQty    string
	PerpetualTickSize       string
	PerpetualPricePrecision string

	FuturesContractNameArr []string
	FuturesMinOrderQty     string
	FuturesTickSize        string
	FuturesPricePrecision  string

	OptionContractNameArr []string
	OptionsMinOrderQty    string
	OptionsTickSize       string
	OptionsPricePrecision string

	IsSpotAvailable      bool
	IsPerpetualAvailable bool
	IsFuturesAvailable   bool
	IsOptionsAvailable   bool
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
	Contract string
	Category string
	Interval string

	Klines     [][]string //массив формат TOHLC
	LastCandle []string   //одна свечка формат TOHLC

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
	WsDeadLineDelta        time.Duration
	WsCmdMsgDeleteDelta    time.Duration
}

type SymbolDataToDb struct {
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
