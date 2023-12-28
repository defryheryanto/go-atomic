package ewallet

import (
	"context"
	"database/sql"
	"errors"

	"github.com/defryheryanto/go-atomic"
)

type User struct {
	ID       int64
	Username string
	Email    string
}

type UserRepository struct {
	db *sql.DB
}

func (r *UserRepository) Insert(ctx context.Context, data *User) (int64, error) {
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

type UserService struct {
	repository     UserRepository
	atomicExecutor atomic.Executor
	walletService  *WalletService
}

// Create() handled user registration which will insert a user and wallet data
func (s *UserService) Create(ctx context.Context, username, email string) error {
	if username == "" {
		return errors.New("empty username")
	}

	// Atomic will ensure if the process returned an error or a panic,
	// it will rollback the transaction
	//
	// If there is no error, the transaction will be committed
	err := s.atomicExecutor.Atomic(ctx, func(ctx context.Context) error {
		userID, err := s.repository.Insert(ctx, &User{
			Username: username,
			Email:    email,
		})
		if err != nil {
			return err
		}

		_, err = s.walletService.Create(ctx, userID, 0)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
