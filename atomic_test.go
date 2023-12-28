package atomic

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func setupTest(t *testing.T) *executor {
	dsn := "postgres://postgres:root@localhost:5432/postgres?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	assert.NoError(t, err)

	return &executor{
		db: db,
	}
}

func TestAtomic(t *testing.T) {
	mockErr := fmt.Errorf("mocked")

	t.Run("already in tx, with error", func(t *testing.T) {
		tx := &sql.Tx{}
		ctx := setTransactionClient(context.Background(), tx)

		e := setupTest(t)
		err := e.Atomic(ctx, func(ctx context.Context) error {
			return mockErr
		})
		assert.Equal(t, mockErr, err)
	})

	t.Run("already in tx, no error", func(t *testing.T) {
		tx := &sql.Tx{}
		ctx := setTransactionClient(context.Background(), tx)

		e := setupTest(t)
		err := e.Atomic(ctx, func(ctx context.Context) error {
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("with error", func(t *testing.T) {
		e := setupTest(t)
		err := e.Atomic(context.Background(), func(ctx context.Context) error {
			return mockErr
		})
		assert.Equal(t, mockErr, err)
	})

	t.Run("no error", func(t *testing.T) {
		e := setupTest(t)
		err := e.Atomic(context.Background(), func(ctx context.Context) error {
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("panic", func(t *testing.T) {
		e := setupTest(t)
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("should receive panic")
			}
		}()
		err := e.Atomic(context.Background(), func(ctx context.Context) error {
			panic("error mock panic")
		})
		assert.NoError(t, err)
	})
}
