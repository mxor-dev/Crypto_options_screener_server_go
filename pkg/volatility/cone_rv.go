package volatility

import (
	"context"
	"crypto_options_screener/pkg/shared"
	"fmt"
	"log/slog"
	"math"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// константы
const (
	candlesPerDay  = 96       // 24 часа * 4 (15-минутки)
	candlesPerYear = 365 * 96 // 35 040 свечей в году для рынка 24/7
)

// LoadCandles выгружает свечи по бирже и монете в хронологическом порядке
func LoadCandles(ctx context.Context, pool *pgxpool.Pool, exchange, contract string, limit int) ([]shared.Candle, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit должен быть позитивным, got %d", limit)
	}

	// Берем последние limit свечей с конца (DESC)
	query := `
		SELECT open_time, open, high, low, close
		FROM kline_data_15m
		WHERE exchange = $1 AND contract = $2
		ORDER BY open_time DESC
		LIMIT $3
	`
	rows, err := pool.Query(ctx, query, exchange, contract, limit)
	if err != nil {
		return nil, fmt.Errorf("Ошибка при запросе свечей: %w", err)
	}

	// автоматически закрывает rows и эффективно аллоцирует память по мере чтения
	candles, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (shared.Candle, error) {
		var c shared.Candle
		err := row.Scan(&c.OpenTime, &c.Open, &c.High, &c.Low, &c.Close)
		return c, err
	})
	if err != nil {
		return nil, fmt.Errorf("Ошибка при чтении свечей: %w", err)
	}

	// Разворачиваем слайс в хронологический порядок (ASC) за O(N/2) без дополнительных аллокаций
	slices.Reverse(candles)

	slog.Info("Получено свечей", "len(candles)", len(candles))

	return candles, nil
}

// Строит конус волатильности для заданных DTE (например, []int{1, 3, 7, 14, 30})
// Также возвращает карту полных временных рядов RV по каждому DTE (пригодится далее для HAR-RV)
// Строит конус волатильности для заданных DTE (например, []int{1, 3, 7, 14, 30})
// Также возвращает карту полных временных рядов RV по каждому DTE (пригодится далее для HAR-RV)
func CalculateVolatilityCone(candles []shared.Candle, dtes []int) (shared.ConeLines, map[int][]float64, error) {
	m := len(candles)
	if m < 2 {
		return shared.ConeLines{}, nil, fmt.Errorf("Ошибка недостаточно свечей: %d", m)
	}

	// Количество шагов доходности = m - 1
	nSteps := m - 1

	// Префиксные суммы длиной nSteps + 1 для расчета за O(1) на любом окне
	prefO := make([]float64, nSteps+1)
	prefO2 := make([]float64, nSteps+1)
	prefC := make([]float64, nSteps+1)
	prefC2 := make([]float64, nSteps+1)
	prefRS := make([]float64, nSteps+1)

	for i := 1; i < m; i++ {
		prevClose := candles[i-1].Close
		o := candles[i].Open
		h := candles[i].High
		l := candles[i].Low
		c := candles[i].Close

		// Защита от нулевых цен
		if prevClose <= 0 || o <= 0 || h <= 0 || l <= 0 || c <= 0 {
			return shared.ConeLines{}, nil, fmt.Errorf("некорректная цена (<= 0) на индексе %d", i)
		}

		oi := math.Log(o / prevClose)
		ci := math.Log(c / o)
		ui := math.Log(h / o)
		di := math.Log(l / o)
		rsi := ui*(ui-ci) + di*(di-ci)

		prefO[i] = prefO[i-1] + oi
		prefO2[i] = prefO2[i-1] + oi*oi
		prefC[i] = prefC[i-1] + ci
		prefC2[i] = prefC2[i-1] + ci*ci
		prefRS[i] = prefRS[i-1] + rsi
	}

	nDtes := len(dtes)
	lines := shared.ConeLines{
		DTE:     make([]int, nDtes),
		Min:     make([]float64, nDtes),
		P10:     make([]float64, nDtes),
		P25:     make([]float64, nDtes),
		Median:  make([]float64, nDtes),
		P75:     make([]float64, nDtes),
		P90:     make([]float64, nDtes),
		Max:     make([]float64, nDtes),
		Current: make([]float64, nDtes),
	}
	seriesMap := make(map[int][]float64, nDtes)

	for i, dte := range dtes {
		window := dte * candlesPerDay
		if window < 2 || window > nSteps {
			return shared.ConeLines{}, nil, fmt.Errorf("недостаточно данных для %d DTE (нужно минимум %d свечей, есть %d)", dte, window+1, m)
		}

		nFloat := float64(window)
		// Весовой коэффициент k Янга-Чжана
		k := 0.34 / (1.34 + (nFloat+1.0)/(nFloat-1.0))

		numWindows := nSteps - window + 1
		rvSeries := make([]float64, numWindows)

		for start := 0; start < numWindows; start++ {
			end := start + window

			sumO := prefO[end] - prefO[start]
			sumO2 := prefO2[end] - prefO2[start]
			varO := (sumO2 - (sumO*sumO)/nFloat) / (nFloat - 1.0)

			sumC := prefC[end] - prefC[start]
			sumC2 := prefC2[end] - prefC2[start]
			varC := (sumC2 - (sumC*sumC)/nFloat) / (nFloat - 1.0)

			sumRS := prefRS[end] - prefRS[start]
			varRS := sumRS / nFloat

			// Итоговая дисперсия Янга-Чжана
			varYZ := varO + k*varC + (1.0-k)*varRS
			if varYZ < 0 {
				varYZ = 0 // защита от погрешности float на плоских участках
			}

			// Аннуализация в процентах
			rvSeries[start] = math.Sqrt(float64(candlesPerYear)*varYZ) * 100.0
		}

		// Сохраняем хронологический ряд для последующего обучения HAR-RV
		seriesMap[dte] = rvSeries

		// Актуальная RV — последний элемент скользящего окна
		currentRV := rvSeries[len(rvSeries)-1]

		// Копируем и сортируем ряд для расчета перцентилей
		sorted := slices.Clone(rvSeries)
		slices.Sort(sorted)

		lines.DTE[i] = dte
		lines.Min[i] = sorted[0]
		lines.P10[i] = percentile(sorted, 0.10)
		lines.P25[i] = percentile(sorted, 0.25)
		lines.Median[i] = percentile(sorted, 0.50)
		lines.P75[i] = percentile(sorted, 0.75)
		lines.P90[i] = percentile(sorted, 0.90)
		lines.Max[i] = sorted[len(sorted)-1]
		lines.Current[i] = currentRV
	}

	return lines, seriesMap, nil
}

// вычисляет квантиль с линейной интерполяцией на отсортированном слайсе
func percentile(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}

	pos := q * float64(n-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return sorted[lower]
	}

	weight := pos - float64(lower)
	return sorted[lower]*(1.0-weight) + sorted[upper]*weight
}

// пример использования
func RunExample(ctx context.Context, pool *pgxpool.Pool) error {

	// candles, err := LoadCandles(ctx, pool, "Bybit", "BTCUSDT", 35000)
	// if err != nil {
	// 	return err
	// }

	// dtes := []int{1, 3, 7, 14, 30}
	// cone, seriesMap, err := CalculateVolatilityCone(candles, dtes)
	// if err != nil {
	// 	return err
	// }

	// fmt.Println("DTE |   Min   |   P25   |  Median |   P75   |   Max   | Current")
	// fmt.Println("----+---------+---------+---------+---------+---------+--------")
	// for _, pt := range cone {
	// 	fmt.Printf("%3d | %6.1f%% | %6.1f%% | %6.1f%% | %6.1f%% | %6.1f%% | %6.1f%%\n",
	// 		pt.DTE, pt.Min, pt.P25, pt.Median, pt.P75, pt.Max, pt.Current)
	// }

	// forecasts, err := PredictHARRV(seriesMap, []int{1, 3, 7})
	// if err != nil {
	// 	return err
	// }

	// fmt.Println("\nHAR-RV Forecast:")
	// fmt.Println("Horizon | Forecast RV |   b0   |  b_1d  |  b_7d  |  b_30d")
	// fmt.Println("--------+-------------+--------+--------+--------+--------")
	// for _, f := range forecasts {
	// 	fmt.Printf("%5d d | %9.1f%% | %6.3f | %6.3f | %6.3f | %6.3f\n",
	// 		f.HorizonDays, f.ForecastRV, f.Weights[0], f.Weights[1], f.Weights[2], f.Weights[3])
	// }

	return nil
}
