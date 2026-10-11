package shared

import (
	"time"

	"crypto_options_screener/pkg/exchanges/exchange_client"
)

// интервалы запроса данных
type Intervals struct {
	RestDataGetInterval    time.Duration
	LinearDataGetInterval  time.Duration
	OptionsDataGetInterval time.Duration
}

// символы
type Symbols struct {
	Symbol    string
	DataToGet []string
}

// биржа
type Exchange struct {
	IsActive  bool
	Name      string
	Symbols   []Symbols
	Intervals Intervals
	Client    exchange_client.Client
}
