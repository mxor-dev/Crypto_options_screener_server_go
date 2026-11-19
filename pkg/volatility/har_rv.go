package volatility

import (
	"fmt"
	"math"
)

// HARForecast содержит прогноз волатильности и веса модели для конкретного горизонта
type HARForecast struct {
	HorizonDays int        `json:"horizon_days"` // 1, 3 или 7 дней
	ForecastRV  float64    `json:"forecast_rv"`  // Прогноз годовой волатильности (%)
	Weights     [4]float64 `json:"weights"`      // Коэффициенты: [beta0, beta_1d, beta_7d, beta_30d]
	SamplesUsed int        `json:"samples_used"` // Количество точек истории для обучения
}

// PredictHARRV обучает модель Log-HAR-RV на исторических рядах seriesMap
// и возвращает прогноз волатильности на заданные горизонты (например, []int{1, 3, 7})
func PredictHARRV(seriesMap map[int][]float64, horizons []int) ([]HARForecast, error) {
	rv1, ok1 := seriesMap[1]
	rv7, ok7 := seriesMap[7]
	rv30, ok30 := seriesMap[30]
	if !ok1 || !ok7 || !ok30 {
		return nil, fmt.Errorf("для HAR-RV в seriesMap обязательно должны быть ряды для 1, 7 и 30 DTE")
	}

	// Длина самого короткого ряда (30 DTE) определяет общее число выровненных точек истории
	nAligned := len(rv30)
	if nAligned == 0 {
		return nil, fmt.Errorf("пустой ряд для 30 DTE")
	}

	// Смещения для выравнивания рядов 1 DTE и 7 DTE с конца к 30 DTE
	offset1 := len(rv1) - nAligned
	offset7 := len(rv7) - nAligned

	// Актуальные значения на текущий момент (последний элемент каждого ряда)
	curLog1 := math.Log(rv1[len(rv1)-1])
	curLog7 := math.Log(rv7[len(rv7)-1])
	curLog30 := math.Log(rv30[len(rv30)-1])

	forecasts := make([]HARForecast, 0, len(horizons))

	for _, h := range horizons {
		targetSeries, ok := seriesMap[h]
		if !ok {
			return nil, fmt.Errorf("в seriesMap отсутствует ряд для целевого горизонта %d DTE", h)
		}

		offsetTarget := len(targetSeries) - nAligned
		shiftSteps := h * candlesPerDay // Сдвиг вперед на h дней (h * 96 свечей)

		// Максимальный индекс t, для которого известно будущее значение t + shiftSteps
		trainLen := nAligned - shiftSteps
		if trainLen < 100 {
			return nil, fmt.Errorf("недостаточно точек для обучения на горизонт %d дней (есть %d)", h, trainLen)
		}

		// Собираем нормальные уравнения OLS: (X^T * X) * beta = X^T * Y
		var xtx [4][4]float64
		var xty [4]float64
		validSamples := 0

		for t := 0; t < trainLen; t++ {
			v1 := rv1[offset1+t]
			v7 := rv7[offset7+t]
			v30 := rv30[t]
			vTarget := targetSeries[offsetTarget+t+shiftSteps]

			if v1 <= 0 || v7 <= 0 || v30 <= 0 || vTarget <= 0 {
				continue
			}

			x := [4]float64{
				1.0,
				math.Log(v1),
				math.Log(v7),
				math.Log(v30),
			}
			y := math.Log(vTarget)

			for r := 0; r < 4; r++ {
				xty[r] += x[r] * y
				for c := 0; c < 4; c++ {
					xtx[r][c] += x[r] * x[c]
				}
			}
			validSamples++
		}

		if validSamples < 50 {
			return nil, fmt.Errorf("слишком мало валидных точек (%d) для горизонта %d DTE", validSamples, h)
		}

		beta, err := solveLinear4x4(xtx, xty)
		if err != nil {
			return nil, fmt.Errorf("ошибка решения OLS для %d DTE: %w", h, err)
		}

		// Делаем прогноз на будущее из текущей точки
		predLog := beta[0] + beta[1]*curLog1 + beta[2]*curLog7 + beta[3]*curLog30
		predRV := math.Exp(predLog)

		forecasts = append(forecasts, HARForecast{
			HorizonDays: h,
			ForecastRV:  predRV,
			Weights:     beta,
			SamplesUsed: validSamples,
		})
	}

	return forecasts, nil
}

// solveLinear4x4 решает систему A * x = b размером 4x4 методом Гаусса с выбором главного элемента
func solveLinear4x4(A [4][4]float64, b [4]float64) ([4]float64, error) {
	var x [4]float64

	for i := 0; i < 4; i++ {
		maxRow := i
		for k := i + 1; k < 4; k++ {
			if math.Abs(A[k][i]) > math.Abs(A[maxRow][i]) {
				maxRow = k
			}
		}

		A[i], A[maxRow] = A[maxRow], A[i]
		b[i], b[maxRow] = b[maxRow], b[i]

		if math.Abs(A[i][i]) < 1e-12 {
			return x, fmt.Errorf("вырожденная матрица (мультиколлинеарность признаков)")
		}

		for k := i + 1; k < 4; k++ {
			factor := A[k][i] / A[i][i]
			for j := i; j < 4; j++ {
				A[k][j] -= factor * A[i][j]
			}
			b[k] -= factor * b[i]
		}
	}

	for i := 3; i >= 0; i-- {
		sum := b[i]
		for j := i + 1; j < 4; j++ {
			sum -= A[i][j] * x[j]
		}
		x[i] = sum / A[i][i]
	}

	return x, nil
}
