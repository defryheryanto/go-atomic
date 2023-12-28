package atomic

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTransactionClient(t *testing.T) {
	t.Run("not exists", func(t *testing.T) {
		tx := GetTransactionClient(context.Background())
		assert.Nil(t, tx)
	})

	t.Run("exists", func(t *testing.T) {
		tx := &sql.Tx{}

		ctx := setTransactionClient(context.Background(), tx)
		result := GetTransactionClient(ctx)
		assert.Equal(t, tx, result)
	})
}
