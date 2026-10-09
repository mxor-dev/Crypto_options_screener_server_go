package exchange_client

type Client interface {
	GetName() string
	GetSymbolInfo(symbol string) (SymbolInfo, error) //получает данные по симвоолу
	// GetKline()                 // получает данные по perpetual или споту
	// GetLinear()                //данные по фьючерсам
	// GetOptions()               //данные по опционам
}

type SymbolInfo struct {
	Symbol string

	LinearMinOrderQty  string
	LinearQtyPrecision string
	LinearTickSize     string

	OptionMinOrderQty  string
	OptionQtyPrecision string
	OptionTickSize     string
}
