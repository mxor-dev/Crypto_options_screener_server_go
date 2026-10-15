package convert

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// получает точность из строки
func GetPrecisionFromString(tickSize string) string {
	dotIndex := strings.Index(tickSize, ".")

	//если точки нет (например, "2" или "100"), то точность — "0"
	if dotIndex == -1 {
		return "0"
	}

	//считаем длину хвоста после точки
	precisionInt := len(tickSize) - dotIndex - 1

	// Переводим int в string и возвращаем
	return strconv.Itoa(precisionInt)
}

// получает текущее время и округляет его до 15 мин
func GetRoundedUTCTime() string {
	//получаем текущее время в таймзоне UTC-0
	now := time.Now().UTC()

	//осбросим все что меньше минуты
	now = now.Truncate(time.Minute)

	//считаем остаток минут, который нужно отнять для округления «вниз»
	minutesToSubtract := now.Minute() % 15

	//отнимаем лишние минуты и сбрасываем секунды/наносекунды в ноль
	roundedTime := now.Add(time.Duration(-minutesToSubtract) * time.Minute)

	// 4. Форматируем по шаблону YYYY-MM-DD hh:mm В Go для форматирования используется эталонное время: "2006-01-02 15:04"
	return roundedTime.Format("2006-01-02 15:04")
}

// получает текущее время и округляет его до 15 мин ответ в мс
func GetRoundedUTCTimeMs() int64 {
	//получаем текущее время в таймзоне UTC-0
	now := time.Now().UTC()

	//осбросим все что меньше минуты
	now = now.Truncate(time.Minute)

	//считаем остаток минут, который нужно отнять для округления «вниз»
	minutesToSubtract := now.Minute() % 15

	//отнимаем лишние минуты и сбрасываем секунды/наносекунды в ноль
	roundedTime := now.Add(time.Duration(-minutesToSubtract) * time.Minute)

	// 4. Возвращаем Unix Timestamp в миллисекундах (int64)
	return roundedTime.UnixMilli()
}

// получает время доставки из названия серии - series - строка в формате YYMONTHDD
func SeriesToDeliveryTimeMs(series string) (int64, error) {

	//проверка на длину строки
	switch len(series) {
	case 6:
		//добавим в начало "0"
		series = "0" + series
	case 7:
		//ничего не делаем, все ок
	default:
		return 0, fmt.Errorf("Длина серии отлична от 6 или 7: %s", series)
	}

	//какрта для перевоад месяцев
	monthsMap := map[string]string{
		"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
		"JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
	}

	//выделим составляющие
	expiryDay := series[0:2]
	monthName := series[2:5]
	expiryYear := "20" + series[5:7]

	//получим месяц "число"
	expiryMonth, exists := monthsMap[monthName]
	if !exists {
		return 0, fmt.Errorf("неизвестный месяц: %s", monthName)
	}

	// Собираем строку для парсинга
	dateStr := fmt.Sprintf("%s-%s-%sT08:00:00Z", expiryYear, expiryMonth, expiryDay)

	// В Go для парсинга используется эталонное время: "2006-01-02T15:04:05Z07:00"
	layout := "2006-01-02T15:04:05Z"
	t, err := time.Parse(layout, dateStr)
	if err != nil {
		return 0, fmt.Errorf("Ошибка парсинга даты: %w", err)
	}

	//получим время в милисекундах
	deliveryTimeMs := t.UnixMilli()

	// slog.Debug("Выполнена конвертация", "func", "SeriesToDeliveryTimeMs", "series", series, "deliveryTimeMs", deliveryTimeMs)

	return deliveryTimeMs, nil
}
