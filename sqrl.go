// package sqrl provides a fluent SQL generator.
//
// See https://github.com/elgris/sqrl for examples.
package sqrl

import (
	"context"
	"github.com/clevabit/utils-go/instapgxpool"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

// Sqlizer is the interface that wraps the ToSql method.
//
// ToSql returns a SQL representation of the Sqlizer, along with a slice of args
// as passed to e.g. database/sql.Exec. It can also return an error.
type Sqlizer interface {
	ToSql() (string, []interface{}, error)
}

// ExecWithContext Execs the SQL returned by s with db.
func ExecWithContext(ctx context.Context, pool instapgxpool.Pool, s Sqlizer) (cmtTag pgconn.CommandTag, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return pool.Exec(ctx, query, args...)
}

// QueryWithContext Querys the SQL returned by s with db.
func QueryWithContext(ctx context.Context, pool instapgxpool.Pool, s Sqlizer) (rows pgx.Rows, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return pool.Query(ctx, query, args...)
}

// QueryRowWithContext QueryRows the SQL returned by s with db.
func QueryRowWithContext(ctx context.Context, pool instapgxpool.Pool, s Sqlizer) RowScanner {
	query, args, err := s.ToSql()
	return &Row{RowScanner: pool.QueryRow(ctx, query, args...), err: err}
}

