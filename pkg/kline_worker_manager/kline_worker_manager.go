package shared

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// KlineWorkerInstance хранит состояние, локальные данные и таймер дедлайна конкретного воркера свечей
type KlineWorkerInstance struct {
	CancelFunc context.CancelFunc

	// dataMu защищает локальные данные воркера свечей от состояния гонки
	dataMu     sync.RWMutex
	LatestData string // Переменная, где Kline-воркер хранит последние полученные данные

	// Управление временем жизни (TTL) воркера
	ttlTimer *time.Timer
	resetTTL chan time.Duration
}

// KlineWorkerManager управляет жизненным циклом всех Kline-воркеров
type KlineWorkerManager struct {
	mu           sync.RWMutex
	ctx          context.Context                 // Глобальный контекст приложения для корректной остановки
	klineWorkers map[string]*KlineWorkerInstance // Хэш-таблица запущенных Kline-воркеров
	ttlDur       time.Duration                   // Время жизни воркера без входящих запросов
}

// NewKlineWorkerManager инициализирует менеджер Kline-воркеров
func NewKlineWorkerManager(ctx context.Context, ttlDuration time.Duration) *KlineWorkerManager {
	return &KlineWorkerManager{
		ctx:          ctx,
		klineWorkers: make(map[string]*KlineWorkerInstance),
		ttlDur:       ttlDuration,
	}
}

// makeKey собирает уникальный составной ключ: "exchange:contract:category"
func (kwm *KlineWorkerManager) makeKey(exchange, contract, category string) string {
	return fmt.Sprintf("%s:%s:%s", exchange, contract, category)
}

// GetKlineDataOrStart возвращает последние данные Kline или лениво запускает воркер, если он не работал
func (kwm *KlineWorkerManager) GetKlineDataOrStart(exchange, contract, category string) (string, bool) {
	key := kwm.makeKey(exchange, contract, category)

	kwm.mu.Lock()
	worker, exists := kwm.klineWorkers[key]

	// СЦЕНАРИЙ 1: Kline-воркер еще не запущен. Стартуем его.
	if !exists {
		ctx, cancel := context.WithCancel(kwm.ctx)

		worker = &KlineWorkerInstance{
			CancelFunc: cancel,
			ttlTimer:   time.NewTimer(kwm.ttlDur),
			resetTTL:   make(chan time.Duration, 1),
			LatestData: "Данные Kline подготавливаются...", // Заглушка для первого запроса
		}
		kwm.klineWorkers[key] = worker
		kwm.mu.Unlock()

		// Запускаем асинхронный воркер свечей
		go kwm.runKlineWorker(ctx, worker, exchange, contract, category)

		return worker.LatestData, false
	}
	kwm.mu.Unlock()

	// СЦЕНАРИЙ 2: Kline-воркер уже работает.
	// Сбрасываем таймер дедлайна, продлевая ему жизнь.
	select {
	case worker.resetTTL <- kwm.ttlDur:
	default:
		// Если канал заполнен, значит запрос на продление уже отправлен
	}

	// Безопасно забираем данные из локальной переменной воркера под RLock
	worker.dataMu.RLock()
	data := worker.LatestData
	worker.dataMu.RUnlock()

	return data, true
}

// runKlineWorker — внутренний изолированный цикл работы конкретной горутины Kline-воркера
func (kwm *KlineWorkerManager) runKlineWorker(ctx context.Context, worker *KlineWorkerInstance, exchange, contract, category string) {
	key := kwm.makeKey(exchange, contract, category)
	fmt.Printf("[СТАРТ] KlineWorker запущен по требованию: %s\n", key)

	defer func() {
		worker.ttlTimer.Stop() // Предотвращаем утечку ресурсов таймера

		kwm.mu.Lock()
		delete(kwm.klineWorkers, key)
		kwm.mu.Unlock()

		fmt.Printf("[ВЫКЛЮЧЕНИЕ] KlineWorker автоматически отключен по TTL-дедлайну: %s\n", key)
	}()

	// Интервал опроса биржи (например, каждые 2 секунды)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Выход при закрытии всего приложения (Graceful Shutdown)
			fmt.Printf("[ОТМЕНА] Глобальная остановка приложения. Завершаем KlineWorker: %s\n", key)
			return

		case <-worker.ttlTimer.C:
			// Сработал TTL-дедлайн. Пользователи больше не запрашивают этот инструмент.
			return

		case newDur := <-worker.resetTTL:
			// Пользователь совершил повторный запрос — сбрасываем таймер
			if !worker.ttlTimer.Stop() {
				select {
				case <-worker.ttlTimer.C:
				default:
				}
			}
			worker.ttlTimer.Reset(newDur)

		case <-ticker.C:
			// Имитация чистой функции сбора рыночных свечей (Kline) с биржи
			freshKlineData := fmt.Sprintf("Свечи от %s [%s:%s] обновлены в %s",
				exchange, contract, category, time.Now().Format("15:04:05"))

			// Блокируем мьютекс данных воркера и обновляем его внутреннюю переменную
			worker.dataMu.Lock()
			worker.LatestData = freshKlineData
			worker.dataMu.Unlock()
		}
	}
}

// StopKlineWorker позволяет вручную принудительно остановить конкретный воркер свечей
func (kwm *KlineWorkerManager) StopKlineWorker(exchange, contract, category string) {
	key := kwm.makeKey(exchange, contract, category)

	kwm.mu.Lock()
	instance, exists := kwm.klineWorkers[key]
	if !exists {
		kwm.mu.Unlock()
		return
	}
	kwm.mu.Unlock()

	instance.CancelFunc()
}
