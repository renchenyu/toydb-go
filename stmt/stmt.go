package stmt

import (
	"encoding/binary"
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

const (
	SizeUsername uint = 32
	SizeEmail    uint = 255
	SizeTotal         = 4 + SizeUsername + SizeEmail
)

type SampleRow struct {
	Id       uint32
	Username [SizeUsername]byte
	Email    [SizeEmail]byte
}

func (r *SampleRow) ToBytes() []byte {
	ret := make([]byte, SizeTotal)
	binary.LittleEndian.PutUint32(ret, r.Id)
	copy(ret[4:], r.Username[:])
	copy(ret[4+SizeUsername:], r.Email[:])
	return ret
}
