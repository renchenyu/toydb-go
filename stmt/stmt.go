package stmt

import (
	"fmt"
	"strings"
)

type StatementType uint8

const (
	StatementInsert StatementType = iota
	StatementSelect
)

var stringToStatment = map[string]StatementType{
	"insert": StatementInsert,
	"select": StatementSelect,
}

type Statement struct {
	Type StatementType
}

func PrepareStatment(s string) (*Statement, error) {
	for k, v := range stringToStatment {
		if strings.HasPrefix(s, k) {
			return &Statement{v}, nil
		}
	}
	return nil, fmt.Errorf("unrecognized statment: %q", s)
}

func ExecuteStatement(stmt *Statement) {
	switch stmt.Type {
	case StatementInsert:
		panic("todo!")
	case StatementSelect:
		panic("todo")
	default:
		panic("not happend")
	}
}
