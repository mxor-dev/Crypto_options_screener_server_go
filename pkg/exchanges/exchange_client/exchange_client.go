package exchange_client

type Client interface {
	GetExchangeName() string
	GetSymbolInfo(symbol string) (SymbolInfo, error) //получает данные по симвоолу
	// GetKline()                 // получает данные по perpetual или споту
	// GetLinear()                //данные по фьючерсам
	// GetOptions()               //данные по опционам
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
