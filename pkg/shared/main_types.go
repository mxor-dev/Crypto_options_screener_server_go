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

// биржа
type Exchange struct {
	IsActive  bool
	Name      string
	Symbols   []string
	Intervals Intervals
	Client    exchange_client.Client
}
