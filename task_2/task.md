# Скопировать таблицу Postgres

Есть два сервера `PostgreSQL`:
* `PROD` — OLTP;
* `STATS` — для аналитики.

В PROD есть большая таблица `profiles(id BIGSERIAL, data JSONB)` с возможными пропусками в `id`.

## Что нужно сделать: 
Реализовать конкурентное копирование данных из `profiles` на `STATS` батчами с восстановлением после сбоев:
* `full=true` — перелить все заново;
* `full=false` — продолжить с места последнего успешного копирования.

## Гарантии/допущения:
* Реализация `Database` сама восстанавливает подключение;
* Реализация `Database` работает с таблицей `profiles` по умолчанию;
* `SaveRows` идемпотентен;
* Можно хранить прогресс любым способом (например, в служебной таблице на `STATS` или локально) — по вашему выбору.

## Требования:
* Разделение на читателей и писателей:
    * Читатели читают батчи из `PROD` и кладут их в очередь `batchQueue`;
    * Писатели берут батчи из очереди и сохраняют в `STATS`;
* Время выполнения функции `CopyTable` должно быть ограничено;
* Время выполнения каждого запроса к бд должно быть ограничено;
* Необходимо ретраить запросы к бд при получении временных ошибок;
* Корректное восстановление после ошибок;
* Разрешается при необходимости использовать `database/sql` и/или расширить интерфейс.

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
func CopyTable(connPool ConnectionPool, fromName string, toName string, full bool) error {
// ... your code
}
```
