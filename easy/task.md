# Скопировать таблицу Postgres

Есть два сервера `PostgreSQL`:
* `PROD` — OLTP;
* `STATS` — для аналитики.

В PROD есть большая таблица `profiles(id BIGSERIAL, data JSONB)`.

## Что нужно сделать:
Реализовать последовательное копирование данных из `profiles` в `STATS` батчами.

## Типы и интерфейсы

### Row

```golang
type Row []interface{}
```
Представляет собой одну строку таблицы.
Элементы массива соответствуют колонкам таблицы.

### Database

```golang
type Database interface {
	io.Closer
	GetMaxID(ctx context.Context) (uint64, error)
	LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error)
	SaveRows(ctx context.Context, rows []Row) error
}
```
Интерфейс для работы с базой данных:
* `io.Closer` — нужен для корректного закрытия подключения;
* `GetMaxID(ctx)` — возвращает максимальный `id` в таблице `profiles`;
* `LoadRows(ctx, minID, maxID)` — загружает строки с id в диапазоне `[minID, maxID)`;
* `SaveRows(ctx, rows)` — сохраняет массив строк в целевую базу.


### ConnectionPool

```golang
type ConnectionPool interface {
	Connect(ctx context.Context, dbname string) (Database, error)
}
```

Пулл подключений к базам данных:
* `Connect(ctx, dbname)` — возвращает объект `Database` для работы с указанной базой.


### Config

```golang
type Config struct {
BatchSize       uint64        // размер батча строк
MaxRetries      int           // максимальное количество retry при временных ошибках
RetriesDelay    time.Duration // задержка между retry
CallTimeout     time.Duration // таймаут выполнения одного запроса
FullCopyTimeout time.Duration // таймаут всей операции копирования
}
```

Используется для параметризации функции `CopyTable`:
* `BatchSize` — определяет, сколько строк считывать за один раз;
* `MaxRetries` и `RetriesDelay` — нужны для механизма повторных попыток при временных ошибках;
* `CallTimeout` — ограничивает выполнение каждого запроса;
* `FullCopyTimeout` — ограничивает выполнение всей функции.

## Гарантии/допущения:
* Реализация `Database` сама восстанавливает подключение;
* Реализация `Database` работает с таблицей `profiles` по умолчанию;
* Метод `SaveRows` идемпотентен.

## Требования:
* Последовательное копирование батчами;
* Время выполнения функции `CopyTable` должно быть ограничено значением `FullCopyTimeout` из конфига;
* Время выполнения каждого запроса к бд должно быть ограничено значением `CallTimeout` из конфига;
* Необходимо retry-ить запросы к бд при получении временных ошибок:
    * Количество retry-ев должно быть ограничено значением `MaxRetries` из конфига;
    * Время ожидания между retry-ями должно быть равно значению `RetriesDelay` из конфига;
    * Временными ошибками являются исключительно timeout'ы выполнений запросов к бд.
* Необходимо корректно обрабатывать ошибки и завершать приложение.

## Стартовый шаблон:
```golang
package main

import (
  "context"
  "io"
)

type Row []interface{}

type Database interface {
  // реализация интерфейса Database умеет переустанавливать подключения
  // вызов SaveRows идемпотентен

  io.Closer
  GetMaxID(ctx context.Context) (uint64, error)
  LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error) // [minID, maxID)
  SaveRows(ctx context.Context, rows []Row) error
}

type ConnectionPool interface {
  Connect(ctx context.Context, dbname string) (Database, error)
}

/*
type Config struct {
	BatchSize       uint64        // размер батча
	MaxRetries      int           // максимальное количество retry-ев
	RetriesDelay    time.Duration // время ожидания между retry-ями
	CallTimeout     time.Duration // максимальное время выполнения запроса к бд
	FullCopyTimeout time.Duration // максимальное время выполнения функции CopyTable
}
*/

// CopyTable копирует таблицу profiles из fromName в toName.
func CopyTable(cfg Config, connPool ConnectionPool, fromName string, toName string) error {
  // ... your code
}
```
