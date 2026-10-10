package convert

import (
	"strconv"
	"strings"
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
