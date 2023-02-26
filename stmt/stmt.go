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
	Type        StatementType
	RowToInsert SampleRow
}

func PrepareStatment(s string) (*Statement, error) {
	for k, v := range stringToStatment {
		if strings.HasPrefix(s, k) {
			var row SampleRow
			switch v {
			case StatementInsert:
				_, err := fmt.Sscanf(s, "insert %d %s %s", &row.Id, &row.Username, &row.Email)
				if err != nil {
					return nil, fmt.Errorf("syntax error: %w", err)
				}
			case StatementSelect:
				// do nothing
			}

			return &Statement{
				Type:        v,
				RowToInsert: row,
			}, nil
		}
	}
	return nil, fmt.Errorf("unrecognized statment: %q", s)
}

func ExecuteStatement(stmt *Statement, table *Table) error {
	switch stmt.Type {
	case StatementInsert:
		err := table.Insert(stmt.RowToInsert)
		if err != nil {
			return err
		}
	case StatementSelect:
		for i := uint(0); i < table.Rows(); i++ {
			row, _ := table.GetRow(i)
			fmt.Println(row)
		}
	default:
		panic("not happend")
	}
	return nil
}

const (
	SizeUsername uint = 32
	SizeEmail    uint = 255
	SizeTotal         = 4 + (4 + SizeUsername) + (4 + SizeEmail)

	OffsetId          = 0
	OffsetUsernameLen = OffsetId + 4
	OffsetUsername    = OffsetUsernameLen + 4
	OffsetEmailLen    = 8 + SizeUsername
	OffsetEmail       = OffsetEmailLen + 4
)

type SampleRow struct {
	Id       uint32
	Username string
	Email    string
}

func (r *SampleRow) String() string {
	return fmt.Sprintf("(%d, %s, %s)", r.Id, r.Username, r.Email)
}

func (r *SampleRow) ToBytes() []byte {
	ret := make([]byte, SizeTotal)
	binary.LittleEndian.PutUint32(ret[OffsetId:], r.Id)

	binary.LittleEndian.PutUint32(ret[OffsetUsernameLen:], uint32(len(r.Username)))
	copy(ret[OffsetUsername:], []byte(r.Username))

	binary.LittleEndian.PutUint32(ret[OffsetEmailLen:], uint32(len(r.Email)))
	copy(ret[OffsetEmail:], []byte(r.Email))

	return ret
}

func NewSampleRowFromBytes(data []byte) *SampleRow {
	if len(data) != int(SizeTotal) {
		panic("invalid data length")
	}
	id := binary.LittleEndian.Uint32(data[OffsetId:])
	usernameLen := binary.LittleEndian.Uint32(data[OffsetUsernameLen:])
	emailLen := binary.LittleEndian.Uint32(data[OffsetEmailLen:])

	return &SampleRow{
		Id:       id,
		Username: string(data[OffsetUsername : OffsetUsername+usernameLen]),
		Email:    string(data[OffsetEmail : OffsetEmail+uint(emailLen)]),
	}
}

const (
	PageSize    uint = 4096
	MaxPages    uint = 100
	RowsPerPage uint = PageSize / SizeTotal
	MaxRows     uint = RowsPerPage * PageSize
)

type Page []byte

type Table struct {
	rows  uint
	pages [MaxPages]Page
}

func NewTable() *Table {
	return &Table{
		rows:  0,
		pages: [100]Page{},
	}
}

func (t *Table) Rows() uint {
	return t.rows
}

func (t *Table) Insert(row SampleRow) error {
	if t.rows == MaxRows {
		return fmt.Errorf("table is full")
	}

	t.rows++

	pageOffset := (t.rows - 1) / RowsPerPage
	rowOffset := (t.rows - 1) % RowsPerPage * SizeTotal

	if t.pages[pageOffset] == nil {
		t.pages[pageOffset] = make([]byte, PageSize)
	}

	rowBytes := row.ToBytes()
	copy(t.pages[pageOffset][rowOffset:], rowBytes)
	return nil
}

func (t *Table) GetRow(idx uint) (*SampleRow, error) {
	if idx > t.rows {
		return nil, fmt.Errorf("not found")
	}

	pageOffset := idx / RowsPerPage
	rowOffset := idx % RowsPerPage * SizeTotal

	data := t.pages[pageOffset][rowOffset : rowOffset+SizeTotal]
	return NewSampleRowFromBytes(data), nil
}
