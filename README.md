Transaction wrapper
===================

This package provides helper code for automatic retries of database
transactions. It is primarily intended to be used against sqlc-generated code.

While not strictly necessary, this README assumes you have a `pkg/database`
directory containing your `slqc.yaml`, schema and queries; the `sqlc.yaml`
is configured such that your generated Go files are at `pkg/database/gendb`
within package gendb. If you have a different structure, modify your import
paths below as appropriate.

To get started, create a `pkg/database/db.go`. First, define a `func Init()`
which creates your database pool and then calls `trxwrap.Init()`, passing your
database handle and a function that wraps a transaction handle within your
sqlc-generated Queries object:

```golang
package database

import (
  // ...

  "github.com/Jille/trxwrap"
  "github.com/you/yourproject/pkg/database/gendb"
)

var (
  db trxwrap.TrxWrap[gendb.Queries]
)

func Init() {
  // ...
  pgx, err := pgxpool.ConnectConfig(context.Background(), pgxConfig)
  if err != nil {
    log.Fatal(err)
  }
  db = trxwrap.New(pgx, func(tx trxwrap.PGDBTX) *gendb.Queries {
    return gendb.New(tx)
  }
}
```

Then, after this, you can wrap the transaction functions e.g. as:

```golang
func RunRWTransaction(ctx context.Context, isolationLevel pgx.TxIsoLevel, runner func(*gendb.Queries) error) error {
	return db.RunRWTransaction(ctx, isolationLevel, runner)
}

func RunROTransaction(ctx context.Context, isolationLevel pgx.TxIsoLevel, runner func(*gendb.Queries) error) error {
	return db.RunROTransaction(ctx, isolationLevel, runner)
}
```

Now, you can write your transactions like this:

```golang
func GetStudents(ctx context.Context, enrolled bool) ([]Student, error) {
  var res []Student
  err := database.RunRWTransaction(ctx, pgx.RepeatableRead, func(q *gendb.Queries) error {
    // Remember that the transaction can be re-run, so we need to start over
    res = nil

    students, err := q.GetStudents(ctx, gendb.GetStudentsParams{
      Enrolled: enrolled,
    })
    if err != nil {
      return err
    }

    for _, student := range students {
      res = append(res, Student{
        Name:      student.Name,
        Birthdate: student.Birthdate,
      })
    }

    return nil
  })
  return res, err
}
```

If your transaction fails because of a transient error, e.g. a database
instance restarting or a TCP connection drop, the transaction will be retried
after a while, performing at most 3 attempts. The error of the last attempt
will be returned.
