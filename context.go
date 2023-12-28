package atomic

import (
	"context"
	"database/sql"
)

type key string

const txKey key = key("transaction_tx_key")

func setTransactionClient(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

// Extract the current transaction from the context if there is any
func GetTransactionClient(ctx context.Context) *sql.Tx {
	tx, ok := ctx.Value(txKey).(*sql.Tx)
	if !ok {
		return nil
	}

	return tx
}
