package sqrl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidArray(t *testing.T) {
	valid := []struct {
		op    Sqlizer
		sql   string
		value string
	}{
		{Array([]string{}), "?", "{}"},
		{Array([]int{}), "?", "{}"},
		{Array([]float32{}), "?", "{}"},
		{Array([]float64{}), "?", "{}"},
		{Array([][]int{}), "?", "{}"},
		{Array([]string{"foo", "bar", "\"quoted\""}), "?", `{"foo","bar","\"quoted\""}`},
		{Array([]int{6, 7, 42}), "?", `{6,7,42}`},
		{Array([]uint8{6, 7, 42}), "?", `{6,7,42}`},
		{Array([][]int{{1, 2}, {3, 4}}), "?", `{{1,2},{3,4}}`},
		{Array([2][2]int{{1, 2}, {3, 4}}), "?", `{{1,2},{3,4}}`},
		{Array([]float32{1.5, 2, 3}), "?", `{1.5,2,3}`},
		{Array([]float64{1.5, 2, 3}), "?", `{1.5,2,3}`},
	}

	for _, test := range valid {
		sql, args, err := test.op.ToSql()

		assert.NoError(t, err, "Unexpected error at case %v", test.op)
		assert.Equal(t, test.sql, sql)
		assert.Equal(t, []interface{}{test.value}, args)
	}
}

func TestInvalidArray(t *testing.T) {
	invalid := []Sqlizer{
		Array([]struct{}{{}}),
		Array(42),
		Array("foo"),
		Array([]interface{}{6, 7, "foo"}),
		Array([][]interface{}{}),
		Array([][]interface{}{{1}}),
	}

	for _, test := range invalid {
		_, _, err := test.ToSql()
		assert.NotNil(t, err, "Expected error at case %+v", test)
	}
}

func ExampleArray() {
	sql, args, err := Insert("posts").
		Columns("content", "tags").
		Values("Lorem Ipsum", Array([]string{"foo", "bar"})).
		PlaceholderFormat(Dollar).
		ToSql()

	if err != nil {
		panic(err)
	}

	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// INSERT INTO posts (content,tags) VALUES ($1,$2)
	// [Lorem Ipsum {"foo","bar"}]
}
