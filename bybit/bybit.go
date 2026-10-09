package bybit

import (
	"encoding/json"
	"fmt"
	"net/http"

	"crypto_options_screener/exchange_client"
)

// базовая точка доступа
const baseEndPont = "https://api.bybit.com"

// ------- СТРУКТУРА
type BybitClient struct {
	Name      string
	apiKey    string
	apiSecret string
}

// ------- КОНСТРУКТОР
func New(apiKey, apiSecret string) *BybitClient {
	return &BybitClient{
		Name:      "bybit",
		apiKey:    apiKey,
		apiSecret: apiSecret,
	}
}

//------- МЕТОДЫ

func (b *BybitClient) GetName() string {
	return b.Name
}

func (b *BybitClient) GetSymbolInfo(symbol string) (exchange_client.SymbolInfo, error) {

	fmt.Printf("Bybit: запрашиваем данные по %s...\n", symbol)

	url := fmt.Sprintf("%s/v5/market/instruments-info?baseCoin=%s&category=%s", baseEndPont, symbol, "")

	resp, err := http.Get(url)
	if err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("Сетевая ошибка: %w", err)
	}
	defer resp.Body.Close()

	//переменная под данные сервера
	var data any

	// Парсим данные в формате JSON
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return exchange_client.SymbolInfo{}, fmt.Errorf("ошибка парсинга: %w", err)
	}

	// Выводим структуру «как она есть» в консоль
	// Спецификатор %+v выведет карту вместе с названиями ключей
	fmt.Printf("data:\n%+v\n", data)

	//ответ
	result := exchange_client.SymbolInfo{
		Symbol: "BTC",
	}

	return result, nil
}
