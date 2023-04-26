package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// wrappedPool automatically retries queries if applicable.
type wrappedPool struct {
	pool *sqlx.DB
}

func (p wrappedPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var ret sql.Result
	var r retrier
	err := r.retry(ctx, func() (bool, error) {
		var err error
		ret, err = p.pool.ExecContext(ctx, query, args...)
		r.maybeReportQueryError(err, query, args)
		return true, err
	}, false)
	return ret, err
}

func (p wrappedPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var r retrier
	err := r.retry(ctx, func() (bool, error) {
		var err error
		rows, err = p.pool.QueryContext(ctx, query, args...)
		r.maybeReportQueryError(err, query, args)
		return true, err
	}, isReadOnlyQuery(query))
	return rows, err
}

func (p wrappedPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	var row *sql.Row
	var r retrier
	_ = r.retry(ctx, func() (bool, error) {
		row = p.pool.QueryRowContext(ctx, query, args...)
		r.maybeReportQueryError(row.Err(), query, args)
		return true, row.Err()
	}, isReadOnlyQuery(query))
	return row
}

func (p wrappedPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt *sql.Stmt
	var r retrier
	err := r.retry(ctx, func() (bool, error) {
		var err error
		stmt, err = p.pool.PrepareContext(ctx, query)
		r.maybeReportQueryError(err, query, nil)
		return true, err
	}, true)
	return stmt, err
}

// wrappedTransaction executes queries inside a transaction. The entire transaction is automatically retried (see retry() in db.go).
type wrappedTransaction struct {
	tx *sqlx.Tx
	r  *retrier
}

func (t *wrappedTransaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ct, err := t.tx.ExecContext(ctx, query, args...)
	t.r.maybeReportQueryError(err, query, args)
	return ct, err
}

func (t *wrappedTransaction) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	t.r.maybeReportQueryError(err, query, args)
	return rows, err
}

func (t *wrappedTransaction) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	row := t.tx.QueryRowContext(ctx, query, args...)
	t.r.maybeReportQueryError(row.Err(), query, args)
	return row
}

func (t *wrappedTransaction) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	t.r.maybeReportQueryError(err, query, nil)
	return stmt, err
}
