package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

// --- Моковые объекты ---

type MockDB struct {
	mock.Mock
}

func NewMockDB() *MockDB {
	return &MockDB{}
}

func (m *MockDB) GetMaxID(ctx context.Context) (uint64, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockDB) LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error) {
	args := m.Called(ctx, minID, maxID)
	return args.Get(0).([]Row), args.Error(1)
}

func (m *MockDB) SaveRows(ctx context.Context, rows []Row) error {
	args := m.Called(ctx, rows)
	return args.Error(0)
}

func (m *MockDB) Close() error { return nil }

type MockConnectionPool struct {
	from, to *MockDB
}

func NewMockConnectionPool(from, to *MockDB) *MockConnectionPool {
	return &MockConnectionPool{from: from, to: to}
}

func (m *MockConnectionPool) Connect(ctx context.Context, dbname string) (Database, error) {
	switch dbname {
	case "from":
		return m.from, nil
	case "to":
		return m.to, nil
	default:
		return nil, fmt.Errorf("unknown db %s", dbname)
	}
}

// --- Тесты ---

func TestCopyTable_FullHappyPath(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	from.On("GetMaxID", mock.Anything).Return(uint64(3), nil)

	from.On("LoadRows", mock.Anything, uint64(1), uint64(1+batchSize)).
		Return([]Row{{1}, {2}, {3}}, nil)

	to.On("SaveRows", mock.Anything, []Row{{1}, {2}, {3}}).Return(nil)

	err := CopyTable(pool, "from", "to", true)
	require.NoError(t, err)
	from.AssertExpectations(t)
	to.AssertExpectations(t)
}

func TestCopyTable_Incremental(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	from.On("GetMaxID", mock.Anything).Return(uint64(5), nil)
	to.On("GetMaxID", mock.Anything).Return(uint64(2), nil)

	start := uint64(2) + 1
	end := start + batchSize
	from.On("LoadRows", mock.Anything, start, end).Return([]Row{{3}, {4}, {5}}, nil)
	to.On("SaveRows", mock.Anything, []Row{{3}, {4}, {5}}).Return(nil)

	err := CopyTable(pool, "from", "to", false)
	require.NoError(t, err)
	from.AssertExpectations(t)
	to.AssertExpectations(t)
}

func TestCopyTable_RetryOnTemporaryError(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	from.On("GetMaxID", mock.Anything).Return(uint64(1), nil)
	to.On("GetMaxID", mock.Anything).Return(uint64(0), nil)

	from.On("LoadRows", mock.Anything, uint64(1), uint64(1+batchSize)).
		Return([]Row{{1}}, nil)

	// эмуляция retry: первый раз ошибка, второй раз успех
	call := to.On("SaveRows", mock.Anything, []Row{{1}})
	call.Return(&net.DNSError{IsTemporary: true}).Once()
	to.On("SaveRows", mock.Anything, []Row{{1}}).Return(nil).Once()

	err := CopyTable(pool, "from", "to", false)
	require.NoError(t, err)
	from.AssertExpectations(t)
	to.AssertExpectations(t)
}

func TestCopyTable_FatalErrorStops(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	from.On("GetMaxID", mock.Anything).Return(uint64(1), nil)
	to.On("GetMaxID", mock.Anything).Return(uint64(0), nil)

	from.On("LoadRows", mock.Anything, uint64(1), uint64(1+batchSize)).
		Return([]Row{}, errors.New("fatal"))

	err := CopyTable(pool, "from", "to", false)
	require.Error(t, err)
	from.AssertExpectations(t)
	to.AssertExpectations(t)
}

func TestCopyTable_ContextTimeoutCancels(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	from.On("GetMaxID", mock.Anything).Return(uint64(1), nil)
	to.On("GetMaxID", mock.Anything).Return(uint64(0), nil)

	from.On("LoadRows", mock.Anything, uint64(1), uint64(1+batchSize)).
		Return([]Row{{1}}, nil)

	for attempt := 0; attempt < maxRetries; attempt++ {
		to.On("SaveRows", mock.Anything, []Row{{1}}).
			Return(context.DeadlineExceeded)
	}

	err := CopyTable(pool, "from", "to", false)
	require.Error(t, err)
	from.AssertExpectations(t)
	to.AssertExpectations(t)
}

func TestCopyTable_ZeroRowsNoSave(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	from.On("GetMaxID", mock.Anything).Return(uint64(0), nil)
	to.On("GetMaxID", mock.Anything).Return(uint64(0), nil)

	err := CopyTable(pool, "from", "to", true)
	require.NoError(t, err)
	to.AssertNotCalled(t, "SaveRows", mock.Anything, mock.Anything)
}

func TestCopyTable_BatchProcessing(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	totalRows := uint64(2*batchSize + 1)
	from.On("GetMaxID", mock.Anything).Return(totalRows, nil)
	to.On("GetMaxID", mock.Anything).Return(uint64(0), nil)

	for offset := uint64(1); offset <= totalRows; offset += batchSize {
		end := offset + batchSize
		rows := []Row{{offset}}
		from.On("LoadRows", mock.Anything, offset, end).Return(rows, nil)
		to.On("SaveRows", mock.Anything, rows).Return(nil)
	}

	err := CopyTable(pool, "from", "to", false)
	require.NoError(t, err)
	from.AssertExpectations(t)
	to.AssertExpectations(t)
}

func TestCopyTable_SkipEmptyBatch(t *testing.T) {
	from := NewMockDB()
	to := NewMockDB()
	pool := NewMockConnectionPool(from, to)

	totalRows := batchSize + 1
	from.On("GetMaxID", mock.Anything).Return(uint64(totalRows), nil)
	to.On("GetMaxID", mock.Anything).Return(uint64(0), nil)

	from.On("LoadRows", mock.Anything, uint64(1), uint64(1+batchSize)).
		Return([]Row{{uint64(1)}}, nil)
	to.On("SaveRows", mock.Anything, []Row{{uint64(1)}}).Return(nil)

	from.On("LoadRows", mock.Anything, uint64(1+batchSize), uint64(1+batchSize*2)).
		Return([]Row{}, nil)

	err := CopyTable(pool, "from", "to", false)
	require.NoError(t, err)

	from.AssertExpectations(t)
	to.AssertExpectations(t)
}
