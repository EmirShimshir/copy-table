# Скопировать таблицу Postgres

Есть два сервера `PostgreSQL`:
* `PROD` — OLTP;
* `STATS` — для аналитики.

В PROD есть большая таблица `profiles(id BIGSERIAL, data JSONB)`.

## Что нужно сделать: 
Реализовать конкурентное копирование данных из `profiles` в `STATS` батчами.

## Гарантии/допущения:
* Реализация `Database` сама восстанавливает подключение;
* Реализация `Database` работает с таблицей `profiles` по умолчанию;
* Метод `SaveRows` идемпотентен.

## Требования:
* Разделение на читателей и писателей:
    * Читатели читают батчи из `PROD` и кладут их в очередь;
    * Писатели берут батчи из очереди и сохраняют в `STATS`.
* Количество одновременно работающих читателей должно быть ограничено значением `WorkersCount` из конфига;
* Количество одновременно работающих писателей должно быть ограничено значением `WorkersCount` из конфига;
* Время выполнения функции `CopyTable` должно быть ограничено значением `FullCopyTimeout` из конфига;
* Время выполнения каждого запроса к бд должно быть ограничено значением `CallTimeout` из конфига;
* Необходимо retry-ить запросы к бд при получении временных ошибок:
    * Количество retry-ев должно быть ограничено значением `MaxRetries` из конфига;
    * Время ожидания между retry-ями должно быть равно значению `RetriesDelay` из конфига;
    * Временными ошибками являются исключительно timeout'ы выполнений запросов к бд.
* Необходимо корректно обрабатывать ошибки и реализовать graceful shutdown.

## Стартовый шаблон:
```golang
type Row []interface{}

type Database interface {
// реализация интерфейса Database умеет переустанавливать подключения
// вызов SaveRows идемпотентен

    io.Closer
    GetMaxID(ctx context.Context) (uint64, error)
    LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error)  // [minID, maxID)
    SaveRows(ctx context.Context, rows []Row) error
}

type ConnectionPool interface {
    Connect(ctx context.Context, dbname string) (Database, error)
}

const (
  batchSize = ... // размер одного батча (обязательно)
  maxRetries = ... // максимальное количество ретраев (обязательно, не больше 3, чтобы немного ждать при тестах)
  ...
)

// CopyTable
// Если full=false то продолжить переливку данных с места прошлой ошибки
// Если full=true - то перелить все данные
func CopyTable(connPool ConnectionPool, fromName string, toName string) error {
// ... your code
}
```
