# Скопировать таблицу Postgres

Есть два сервера `PostgreSQL`:
* `PROD` — OLTP;
* `STATS` — для аналитики.

В PROD есть большая таблица `profiles(id BIGSERIAL, data JSONB)`.

## Что нужно сделать:
Реализовать последовательное копирование данных из `profiles` в `STATS` батчами.

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
* Необходимо корректно обрабатывать ошибки.

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
