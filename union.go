package sqrl

type unionPart struct {
	expr Sqlizer
	args []interface{}
}

func newUnionPart(pred *SelectBuilder) Sqlizer {
	return &unionPart{expr: pred}
}

func (p unionPart) ToSql() (sql string, args []interface{}, err error) {
	sql, args, err = p.expr.ToSql()
	return
}
