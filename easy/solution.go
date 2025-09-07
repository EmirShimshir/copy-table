package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
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

var errRetriesFailed = errors.New("all retries failed")

// CopyTable копирует таблицу profiles из fromName в toName.
func CopyTable(cfg Config, connPool ConnectionPool, fromName string, toName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.FullCopyTimeout)
	defer cancel()

	fromDB, err := connPool.Connect(ctx, fromName)
	if err != nil {
		return fmt.Errorf("connect from: %w", err)
	}
	defer fromDB.Close()

	toDB, err := connPool.Connect(ctx, toName)
	if err != nil {
		return fmt.Errorf("connect to: %w", err)
	}
	defer toDB.Close()

	var startID uint64 = 1

	endID, err := callWithRetry(ctx, cfg, func(ctx context.Context) (uint64, error) {
		return fromDB.GetMaxID(ctx)
	})
	if err != nil {
		return fmt.Errorf("get max id from source: %w", err)
	}

	if startID >= endID {
		return nil
	}

	for offset := startID; offset <= endID; offset += cfg.BatchSize {
		rows, err := callWithRetry(ctx, cfg, func(ctx context.Context) ([]Row, error) {
			return fromDB.LoadRows(ctx, offset, offset+cfg.BatchSize)
		})
		if err != nil {
			return fmt.Errorf("load rows [%d,%d): %w", offset, offset+cfg.BatchSize, err)
		}
		if len(rows) == 0 {
			continue
		}

		if _, err := callWithRetry(ctx, cfg, func(ctx context.Context) (struct{}, error) {
			err = toDB.SaveRows(ctx, rows)
			return struct{}{}, err
		}); err != nil {
			return fmt.Errorf("save rows [%d,%d): %w", offset, offset+cfg.BatchSize, err)
		}
	}

	return nil
}

func callWithRetry[T any](ctx context.Context, cfg Config, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil { // если внешний контекст завершился, то сразу вернем ошибку
			return zero, ctx.Err()
		}
		ctxTimeout, cancel := context.WithTimeout(ctx, cfg.CallTimeout) // создаем локальный контекст с таймаутом на запрос к бд
		res, err := fn(ctxTimeout)
		cancel() // defer писать нельзя, так как создаем локальный контекст в цикле!
		if err != nil {
			if isTemporary(err) {
				select { // ждем в select, так как RetriesDelay потенциально может быть долгим
				case <-ctx.Done():
					return zero, ctx.Err()
				default:
					<-time.After(cfg.RetriesDelay)
					continue
				}
			}
			return zero, err
		}
		return res, nil
	}
	return zero, errRetriesFailed
}

func isTemporary(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}
