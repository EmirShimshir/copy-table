package main

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"math"
	"math/rand"
	"net"
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

const (
	batchSize        = 1000
	maxRetries       = 3
	baseRetriesDelay = 1 * time.Second
	maxRetriesDelay  = 30 * time.Second
	callTimeout      = 10 * time.Second
	fullCopyTimeout  = 12 * time.Hour
	workerCount      = 4
)

type batch struct {
	rows   []Row
	offset uint64
}

// CopyTable копирует таблицу profiles из fromName в toName.
// full=true — начать с начала (игнорируем данные на STATS).
// full=false — продолжаем с MAX(id) в целевой БД.
func CopyTable(connPool ConnectionPool, fromName string, toName string, full bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), fullCopyTimeout)
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

	maxID, err := callWithRetry(ctx, func(ctx context.Context) (uint64, error) {
		return fromDB.GetMaxID(ctx)
	})
	if err != nil {
		return fmt.Errorf("get max id from source: %w", err)
	}

	var startID uint64 = 0
	if !full {
		lastID, err := callWithRetry(ctx, func(ctx context.Context) (uint64, error) {
			return toDB.GetMaxID(ctx)
		})
		if err != nil {
			return fmt.Errorf("get max id from target: %w", err)
		}
		startID = lastID
	}

	if startID >= maxID {
		return nil
	}

	offsetCh := make(chan uint64, 2*workerCount)
	batchQueue := make(chan batch, 2*workerCount)

	groupMain, ctx := errgroup.WithContext(ctx)

	groupMain.Go(func() error {
		return runOffsetsGenerator(ctx, startID, maxID, offsetCh)
	})

	groupMain.Go(func() error {
		return runReaders(ctx, fromDB, offsetCh, batchQueue)
	})

	groupMain.Go(func() error {
		return runWriters(ctx, toDB, batchQueue)
	})

	return groupMain.Wait()
}

func runOffsetsGenerator(ctx context.Context, startID uint64, maxID uint64, ch chan uint64) error {
	defer close(ch)
	for start := startID + 1; start <= maxID; start += batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- start:
		}
	}
	return nil
}

func runReaders(ctx context.Context, db Database, offsetCh chan uint64, queueCh chan batch) error {
	defer close(queueCh)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workerCount)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case offset, ok := <-offsetCh:
			if !ok {
				return g.Wait()
			}
			g.Go(func() error {
				rows, err := callWithRetry(ctx, func(ctx context.Context) ([]Row, error) {
					return db.LoadRows(ctx, offset, offset+batchSize)
				})
				if err != nil {
					return fmt.Errorf("load rows [%d,%d): %w", offset, offset+batchSize, err)
				}
				if len(rows) > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case queueCh <- batch{
						rows:   rows,
						offset: offset,
					}:
					}
				}
				return nil
			})
		}
	}
}

func runWriters(ctx context.Context, db Database, queue chan batch) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workerCount)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rowsDto, ok := <-queue:
			if !ok {
				return g.Wait()
			}
			g.Go(func() error {
				_, err := callWithRetry(ctx, func(ctx context.Context) (struct{}, error) {
					return struct{}{}, db.SaveRows(ctx, rowsDto.rows)
				})
				if err != nil {
					return fmt.Errorf("save rows [%d,%d): %w", rowsDto.offset, rowsDto.offset+batchSize, err)
				}
				return nil
			})
		}
	}
}

func callWithRetry[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 0; attempt < maxRetries; attempt++ {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		ctxTimeout, cancel := context.WithTimeout(ctx, callTimeout)
		res, err := fn(ctxTimeout)
		cancel()

		if err == nil {
			return res, nil
		}
		if !isTemporary(err) || attempt == maxRetries-1 {
			return zero, err
		}
		backoff := baseRetriesDelay * time.Duration(math.Pow(2, float64(attempt)))
		if backoff > maxRetriesDelay {
			backoff = maxRetriesDelay
		}
		jitter := time.Duration(rand.Int63n(int64(backoff)))

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(jitter):
		}
	}
	return zero, fmt.Errorf("all retries failed")
}

func isTemporary(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && (ne.Timeout()) {
		return true
	}
	return false
}
