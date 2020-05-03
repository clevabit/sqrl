package sqrl

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type expr struct {
	sql  string
	args []interface{}
}

// Expr builds value expressions for InsertBuilder and UpdateBuilder.
//
// Ex:
//     .Values(Expr("FROM_UNIXTIME(?)", t))
func Expr(sql string, args ...interface{}) expr {
	return expr{sql: sql, args: args}
}

func (e expr) ToSql() (string, []interface{}, error) {
	if !hasSqlizer(e.args) {
		return e.sql, e.args, nil
	}

	args := make([]interface{}, 0, len(e.args))
	sql, err := replacePlaceholders(e.sql, func(buf *bytes.Buffer, i int) error {
		if i > len(e.args) {
			buf.WriteRune('?')
			return nil
		}
		switch arg := e.args[i-1].(type) {
		case Sqlizer:
			sql, vs, err := arg.ToSql()
			if err != nil {
				return err
			}
			args = append(args, vs...)
			fmt.Fprintf(buf, sql)
		default:
			args = append(args, arg)
			buf.WriteRune('?')
		}
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	return sql, args, nil
}

type exprs []expr

func (es exprs) AppendToSql(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	for i, e := range es {
		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		_, err := io.WriteString(w, e.sql)
		if err != nil {
			return nil, err
		}
		args = append(args, e.args...)
	}
	return args, nil
}

// aliasExpr helps to alias part of SQL query generated with underlying "expr"
type aliasExpr struct {
	expr  Sqlizer
	alias string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//		.Column(Alias(caseStmt, "case_column"))
func Alias(expr Sqlizer, alias string) aliasExpr {
	return aliasExpr{expr, alias}
}

func (e aliasExpr) ToSql() (sql string, args []interface{}, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("(%s) AS %s", sql, e.alias)
	}
	return
}

// lateralExpr helps to lateral join a part of SQL query generated with underlying "expr"
type lateralExpr struct {
	expr  Sqlizer
	alias string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//		.Column(Alias(caseStmt, "case_column"))
func lateralJoin(expr Sqlizer, alias string) lateralExpr {
	return lateralExpr{expr, alias}
}

func (e lateralExpr) ToSql() (sql string, args []interface{}, err error) {
	sql, args, err = e.expr.ToSql()
	if err == nil {
		sql = fmt.Sprintf("LATERAL (%s) AS %s", sql, e.alias)
	}
	return
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Eq{"id": 1})
type Eq map[string]interface{}

func (eq Eq) toSql(useNotOpr bool) (sql string, args []interface{}, err error) {
	var (
		exprs       []string
		equalOpr    = "="
		inOpr       = "IN"
		nullOpr     = "IS"
		inEmptyExpr = "(1=0)" // Portable FALSE
	)

	if useNotOpr {
		equalOpr = "<>"
		inOpr = "NOT IN"
		nullOpr = "IS NOT"
		inEmptyExpr = "(1=1)" // Portable TRUE
	}

	for key, val := range eq {
		expr := ""

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			expr = fmt.Sprintf("%s %s NULL", key, nullOpr)
		} else {
			if isListType(val) {
				valVal := reflect.ValueOf(val)
				if valVal.Len() == 0 {
					expr = inEmptyExpr
					if args == nil {
						args = []interface{}{}
					}
				} else {
					for i := 0; i < valVal.Len(); i++ {
						args = append(args, valVal.Index(i).Interface())
					}
					expr = fmt.Sprintf("%s %s (%s)", key, inOpr, Placeholders(valVal.Len()))
				}
			} else {
				expr = fmt.Sprintf("%s %s ?", key, equalOpr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	return
}

// ToSql builds the query into a SQL string and bound args.
func (eq Eq) ToSql() (sql string, args []interface{}, err error) {
	return eq.toSql(false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(NotEq{"id": 1}) == "id <> 1"
type NotEq Eq

// ToSql builds the query into a SQL string and bound args.
func (neq NotEq) ToSql() (sql string, args []interface{}, err error) {
	return Eq(neq).toSql(true)
}

// Lt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Lt{"id": 1})
type Lt map[string]interface{}

func (lt Lt) toSql(opposite, orEq bool) (sql string, args []interface{}, err error) {
	var (
		exprs []string
		opr   string = "<"
	)

	if opposite {
		opr = ">"
	}

	if orEq {
		opr = fmt.Sprintf("%s%s", opr, "=")
	}

	for key, val := range lt {
		expr := ""

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with less than or greater than operators")
			return
		} else {
			if isListType(val) {
				err = fmt.Errorf("cannot use array or slice with less than or greater than operators")
				return
			} else {
				expr = fmt.Sprintf("%s %s ?", key, opr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	return
}

func (lt Lt) ToSql() (sql string, args []interface{}, err error) {
	return lt.toSql(false, false)
}

// LtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(LtOrEq{"id": 1}) == "id <= 1"
type LtOrEq Lt

func (ltOrEq LtOrEq) ToSql() (sql string, args []interface{}, err error) {
	return Lt(ltOrEq).toSql(false, true)
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Gt{"id": 1}) == "id > 1"
type Gt Lt

func (gt Gt) ToSql() (sql string, args []interface{}, err error) {
	return Lt(gt).toSql(true, false)
}

// GtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(GtOrEq{"id": 1}) == "id >= 1"
type GtOrEq Lt

func (gtOrEq GtOrEq) ToSql() (sql string, args []interface{}, err error) {
	return Lt(gtOrEq).toSql(true, true)
}

type conj []Sqlizer

func (c conj) join(sep string) (sql string, args []interface{}, err error) {
	var sqlParts []string
	for _, sqlizer := range c {
		partSql, partArgs, err := sqlizer.ToSql()
		if err != nil {
			return "", nil, err
		}
		if partSql != "" {
			sqlParts = append(sqlParts, partSql)
			args = append(args, partArgs...)
		}
	}
	if len(sqlParts) > 0 {
		sql = fmt.Sprintf("(%s)", strings.Join(sqlParts, sep))
	}
	return
}

// And is syntactic sugar that glues where/having parts with AND clause
// Ex:
//     .Where(And{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type And conj

// ToSql builds the query into a SQL string and bound args.
func (a And) ToSql() (string, []interface{}, error) {
	return conj(a).join(" AND ")
}

// Or is syntactic sugar that glues where/having parts with OR clause
// Ex:
//     .Where(Or{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type Or conj

// ToSql builds the query into a SQL string and bound args.
func (o Or) ToSql() (string, []interface{}, error) {
	return conj(o).join(" OR ")
}

func isListType(val interface{}) bool {
	if driver.IsValue(val) {
		return false
	}
	valVal := reflect.ValueOf(val)
	return valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice
}

func hasSqlizer(args []interface{}) bool {
	for _, arg := range args {
		_, ok := arg.(Sqlizer)
		if ok {
			return true
		}
	}
	return false
}

// Between is syntactic sugar for use with BETWEEN methods.
// Ex:
//     .Where(Between{field: "id", left: 1, right: 5}) == "id between 1 and 5"
type Between struct {
	field string
	left  interface{}
	right interface{}
}

func (between Between) ToSql() (sql string, args []interface{}, err error) {
	return fmt.Sprintf("%s BETWEEN ? AND ?", between.field), []interface{}{between.left, between.right}, nil
}

type concatExpr []interface{}

func (ce concatExpr) ToSql() (sql string, args []interface{}, err error) {
	for _, part := range ce {
		switch p := part.(type) {
		case string:
			sql += p
		case Sqlizer:
			pSql, pArgs, err := p.ToSql()
			if err != nil {
				return "", nil, err
			}
			sql += pSql
			args = append(args, pArgs...)
		default:
			return "", nil, fmt.Errorf("%#v is not a string or Sqlizer", part)
		}
	}
	return
}

// ConcatExpr builds an expression by concatenating strings and other expressions.
//
// Ex:
//     name_expr := Expr("CONCAT(?, ' ', ?)", firstName, lastName)
//     ConcatExpr("COALESCE(full_name,", name_expr, ")")
func ConcatExpr(parts ...interface{}) concatExpr {
	return concatExpr(parts)
}

type fn struct {
	name  string
	fargs conj
}

func Fn(name string, args ...Sqlizer) *fn {
	return &fn{name: name, fargs: args}
}

func (fn fn) ToSql() (sql string, args []interface{}, err error) {
	var (
		aSql  string
		aArgs []interface{}
	)

	sql = fn.name + "("
	args = make([]interface{}, 0)
	for a := 0; a < len(fn.fargs); a++ {
		if a > 0 {
			sql += ", "
		}

		aSql, aArgs, err = fn.fargs[a].ToSql()
		if err != nil {
			return
		}

		sql += aSql
		args = append(args, aArgs...)
	}
	sql += ")"

	return
}

type any struct {
	column string
	args   Sqlizer
}

func Any(column string, array interface{}) *any {
	return &any{column: column, args: Array(array)}
}

func (any any) ToSql() (sql string, args []interface{}, err error) {
	p, a, e := any.args.ToSql()
	if e != nil {
		return "", nil, e
	}

	return fmt.Sprintf("%s = ANY(%s)", any.column, p), a, nil
}
