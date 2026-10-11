package convert

import (
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
