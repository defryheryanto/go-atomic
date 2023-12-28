package ewallet

import (
	"context"
	"database/sql"

	"github.com/defryheryanto/go-atomic"
)

type Wallet struct {
	ID     int64
	UserID int64
	Amount float64
}

type WalletRepository struct {
	db *sql.DB
}

func (r *WalletRepository) Insert(ctx context.Context, data *Wallet) (int64, error) {
	execFunc := r.db.ExecContext
	// Get the tx from the context and point to the tx if any to ensure that the process using the transaction
	tx := atomic.GetTransactionClient(ctx)
	if tx != nil {
		execFunc = tx.ExecContext
	}

	_, err := execFunc(ctx, "insert query here")
	if err != nil {
		return 0, err
	}

	return 1, nil
}

type WalletService struct {
	repository *WalletRepository
}

func (s *WalletService) Create(ctx context.Context, userID int64, amount float64) (int64, error) {
	return s.repository.Insert(ctx, &Wallet{
		UserID: userID,
		Amount: amount,
	})
}
