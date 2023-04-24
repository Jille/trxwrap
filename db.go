package trxwrap

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PgxHandle interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type PGDBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

type TransactionRunner[Q any] func(*Q) error

type TrxWrap[Q any] struct {
	db    PgxHandle
	gendb func(PGDBTX) *Q
}

func New[Q any](db PgxHandle, gendb func(PGDBTX) *Q) TrxWrap[Q] {
	return TrxWrap[Q]{
		db:    db,
		gendb: gendb,
	}
}

func (w TrxWrap[Q]) RunRWTransaction(ctx context.Context, isolationLevel pgx.TxIsoLevel, runner TransactionRunner[Q]) error {
	txo := pgx.TxOptions{
		IsoLevel:   isolationLevel,
		AccessMode: pgx.ReadWrite,
	}
	return w.RunTransaction(ctx, txo, false, runner)
}

func (w TrxWrap[Q]) RunROTransaction(ctx context.Context, isolationLevel pgx.TxIsoLevel, runner TransactionRunner[Q]) error {
	txo := pgx.TxOptions{
		IsoLevel:   isolationLevel,
		AccessMode: pgx.ReadOnly,
	}
	return w.RunTransaction(ctx, txo, true, runner)
}

func (t TrxWrap[Q]) RunTransaction(ctx context.Context, txo pgx.TxOptions, idempotent bool, runner TransactionRunner[Q]) error {
	return retry(ctx, func() (bool, error) {
		return t.runTransactionOnce(ctx, txo, runner)
	}, idempotent || txo.AccessMode == pgx.ReadOnly)
}

func retry(ctx context.Context, f func() (bool, error), idempotent bool) error {
	for attempt := 0; ; attempt++ {
		commitAttempted, err := f()
		if attempt >= 3 {
			return err
		}
		retry := pgconn.SafeToRetry(err)
		switch ToSQLState(err) {
		case "40001", "40P01":
			retry = true
		case "08Q99", // io.EOF or io.UnexpectedEOF
			"57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"57P03", // cannot_connect_now
			"08000", // connection_exception
			"08003", // connection_does_not_exist
			"08006", // connection_failure
			"08001", // sqlclient_unable_to_establish_sqlconnection
			"08004": // sqlserver_rejected_establishment_of_sqlconnection
			retry = !commitAttempted || idempotent
		}
		if retry {
			// TODO: Exponential backoff?
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return err
	}
}

func (t TrxWrap[Q]) runTransactionOnce(ctx context.Context, txo pgx.TxOptions, runner TransactionRunner[Q]) (commitAttempted bool, _ error) {
	tx, err := t.db.BeginTx(ctx, txo)
	if err != nil {
		return false, wrapError(err)
	}
	q := t.gendb(wrappedTransaction{tx})
	if err := runner(q); err != nil {
		// TODO: Use multi-error to combine a possible error from rollback with err.
		tx.Rollback(ctx)
		return false, err
	}
	return true, wrapError(tx.Commit(ctx))
}

func ToSQLState(err error) string {
	if e, ok := err.(Error); ok {
		if e.parent == io.EOF || e.parent == io.ErrUnexpectedEOF {
			return "08Q99"
		}
	}
	var pge *pgconn.PgError
	if errors.As(err, &pge) {
		return pge.SQLState()
	}
	return ""
}

// wrapError wraps an error that comes from the database.
// If the given error is nil, nil wil be returned.
// The returned error will hide the actual error when returned through gRPC.
func wrapError(err error) error {
	if err == nil {
		return nil
	}
	if err == pgx.ErrNoRows || err == pgx.ErrTxClosed || err == pgx.ErrTxCommitRollback {
		return err
	}
	return Error{err}
}

type Error struct {
	parent error
}

func (e Error) Error() string {
	return e.parent.Error()
}

func (e Error) GRPCStatus() *status.Status {
	return status.New(codes.Internal, "database error")
}

func (e Error) Unwrap() error {
	return e.parent
}

func isReadOnlyQuery(sql string) bool {
	for strings.HasPrefix(sql, "--") {
		sql = sql[strings.Index(sql, "\n")+1:]
	}
	return strings.HasPrefix(sql, "SELECT")
}
