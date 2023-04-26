package database

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"src.hexon.nl/bummer/v4"
	"src.hexon.nl/jlr-orderevents/database/gendb"
)

const (
	MAXRETRIES  = 3
	RETRYWAIT   = 50 * time.Millisecond
	RETRYJITTER = 5 * time.Millisecond
)

var (
	db              *sqlx.DB
	Transactionless *gendb.Queries
)

type TransactionRunner func(*gendb.Queries) error

func InitDatabase(driver, dsn string, maxConnections int) error {
	var err error
	db, err = sqlx.Connect(driver, dsn)
	if err != nil {
		return err
	}

	// Settings recommended by github.com/go-sql-driver/mysql to configure explicitly
	db.SetConnMaxLifetime(90 * time.Second) // Default timeout on MariaDB servers is 2 minutes
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections)

	Transactionless = gendb.New(wrappedPool{db})

	return nil
}

func RunTransactionless(runner TransactionRunner) error {
	return runner(Transactionless)
}

func RunRWTransaction(ctx context.Context, isolationLevel sql.IsolationLevel, runner TransactionRunner) error {
	txo := sql.TxOptions{
		Isolation: isolationLevel,
		ReadOnly:  false,
	}
	return RunTransaction(ctx, txo, runner)
}

func RunROTransaction(ctx context.Context, isolationLevel sql.IsolationLevel, runner TransactionRunner) error {
	txo := sql.TxOptions{
		Isolation: isolationLevel,
		ReadOnly:  true,
	}
	return RunTransaction(ctx, txo, runner)
}

func RunTransaction(ctx context.Context, txo sql.TxOptions, runner TransactionRunner) error {
	var r retrier
	return r.retry(ctx, func() (bool, error) {
		return runTransactionOnce(ctx, txo, runner, &r)
	}, txo.ReadOnly)
}

type retrier struct {
	error bummer.PendingMessage
}

func (r *retrier) retry(ctx context.Context, f func() (bool, error), idempotent bool) error {
	for attempt := 0; ; attempt++ {
		commitAttempted, err := f()
		if attempt >= MAXRETRIES {
			r.error.Send()
			r.error = bummer.PendingMessage{}
			return err
		}

		retry := false
		switch ToMySQLError(err) {
		case 1205, // Lock wait timeout exceeded; try restarting transaction
			1213, // Deadlock found when trying to get lock; try restarting transaction
			1412, // Table definition has changed, please retry transaction
			1587, // Too many files opened, please execute the command again
			1613, // XA_RBTIMEOUT: Transaction branch was rolled back: took too long
			1614, // XA_RBDEADLOCK: Transaction branch was rolled back: deadlock was detected
			1637, // Too many active concurrent transactions
			1689, // Wait on a lock was aborted due to a pending exclusive lock
			3058: // Deadlock found when trying to get user-level lock; try rolling back transaction/releasing locks and restarting lock acquisition.
			retry = true
		case 1053, // Server shutdown in progress
			1077, // Normal shutdown
			1078, // Got signal %d. Aborting!
			1079: // Shutdown complete
			retry = !commitAttempted || idempotent
		}
		if retry {
			// Exponential backoff
			wait := RETRYWAIT * time.Duration(attempt+1)
			jitter := time.Duration(rand.Int63n(int64(RETRYJITTER)))
			totalWait := wait + jitter

			r.error.DropLevel(bummer.Warning).Send()
			r.error = bummer.PendingMessage{}

			time.Sleep(totalWait)
			continue
		}

		r.error.Send()
		r.error = bummer.PendingMessage{}

		return err
	}
}

func ToMySQLError(err error) uint16 {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

func isReadOnlyQuery(sql string) bool {
	for strings.HasPrefix(sql, "--") {
		sql = sql[strings.Index(sql, "\n")+1:]
	}
	return strings.HasPrefix(sql, "SELECT")
}

func runTransactionOnce(ctx context.Context, txo sql.TxOptions, runner TransactionRunner, r *retrier) (commitAttempted bool, _ error) {
	tx, err := db.BeginTxx(ctx, &txo)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	q := gendb.New(&wrappedTransaction{
		tx: tx,
		r:  r,
	})
	if err := runner(q); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (r *retrier) maybeReportQueryError(err error, query string, args []interface{}) {
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return
	}

	// Send any previous error. In case a query in a transaction kept going instead of bailing out.
	r.error.Send()

	r.error = bummer.Global.NewError().Callsite(true).Backtrace(true).Freezef("Query (%s) (args: %v) failed: %v", query, args, err)
}
