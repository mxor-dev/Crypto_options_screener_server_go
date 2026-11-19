package volatility

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// одна 15-минутную свечу из kline_data_15m
type Candle struct {
	OpenTime time.Time
	Open     float64
	High     float64
	Low      float64
	Close    float64
}

// метрики конуса для конкретного горизонта (DTE)
type ConePoint struct {
	DTE     int     `json:"dte"`
	Min     float64 `json:"min"`
	P10     float64 `json:"p10"`
	P25     float64 `json:"p25"`
	Median  float64 `json:"median"`
	P75     float64 `json:"p75"`
	P90     float64 `json:"p90"`
	Max     float64 `json:"max"`
	Current float64 `json:"current"`
}

// константы
const (
	candlesPerDay  = 96       // 24 часа * 4 (15-минутки)
	candlesPerYear = 365 * 96 // 35 040 свечей в году для рынка 24/7
)

// LoadCandles выгружает свечи по бирже и монете в хронологическом порядке
func LoadCandles(ctx context.Context, pool *pgxpool.Pool, exchange, contract string, limit int) ([]Candle, error) {
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
	candles, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Candle, error) {
		var c Candle
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
func CalculateVolatilityCone(candles []Candle, dtes []int) ([]ConePoint, map[int][]float64, error) {
	m := len(candles)
	if m < 2 {
		return nil, nil, fmt.Errorf("Ошибка недостаточно свечей: %d", m)
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
			return nil, nil, fmt.Errorf("некорректная цена (<= 0) на индексе %d", i)
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

	cone := make([]ConePoint, 0, len(dtes))
	seriesMap := make(map[int][]float64, len(dtes))

	for _, dte := range dtes {
		window := dte * candlesPerDay
		if window < 2 || window > nSteps {
			return nil, nil, fmt.Errorf("недостаточно данных для %d DTE (нужно минимум %d свечей, есть %d)", dte, window+1, m)
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

		cone = append(cone, ConePoint{
			DTE:     dte,
			Min:     sorted[0],
			P10:     percentile(sorted, 0.10),
			P25:     percentile(sorted, 0.25),
			Median:  percentile(sorted, 0.50),
			P75:     percentile(sorted, 0.75),
			P90:     percentile(sorted, 0.90),
			Max:     sorted[len(sorted)-1],
			Current: currentRV,
		})
	}

	return cone, seriesMap, nil
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

	candles, err := LoadCandles(ctx, pool, "Bybit", "BTCUSDT", 35000)
	if err != nil {
		return err
	}

	dtes := []int{1, 3, 7, 14, 30}
	cone, seriesMap, err := CalculateVolatilityCone(candles, dtes)
	if err != nil {
		return err
	}

	for _, pt := range cone {
		fmt.Printf("%2d DTE | Min: %5.1f%% | P25: %5.1f%% | Med: %5.1f%% | P75: %5.1f%% | Max: %5.1f%% | Cur: %5.1f%%\n",
			pt.DTE, pt.Min, pt.P25, pt.Median, pt.P75, pt.Max, pt.Current)
	}

	// seriesMap[1], seriesMap[7], seriesMap[30] уже готовы для следующего шага (HAR-RV)
	_ = seriesMap
	return nil
}
