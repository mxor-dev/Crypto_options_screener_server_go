package shared

type UserData struct {
	Id       string              `json:"id"`
	Name     string              `json:"name"`
	Role     string              `json:"role"`
	FeeRates map[string]FeeRates `json:"fee_rates"`
}

// создает и инициализирует нового пользователя
func NewUserData(allExchanges map[string]*Exchange, id, name, role string) *UserData {

	// Выделяем память сразу под количество бирж
	rates := make(map[string]FeeRates, len(allExchanges))

	for key := range allExchanges {
		// Проверяем, что указатель на биржу не nil, чтобы избежать паники
		if allExchanges[key] != nil {
			rates[key] = allExchanges[key].FeeRates
		}
	}

	return &UserData{
		Id:       id,
		Name:     name,
		Role:     role,
		FeeRates: rates,
	}
}
