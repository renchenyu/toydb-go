package stmt

import (
	"encoding/binary"
	"fmt"
	"strings"
	"toydb-go/bptree_disk"
)

type StatementType uint8

const (
	StatementInsert StatementType = iota
	StatementSelect
	StatementDelete
)

var stringToStatment = map[string]StatementType{
	"insert": StatementInsert,
	"select": StatementSelect,
	"delete": StatementDelete,
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
			case StatementDelete:
				_, err := fmt.Sscanf(s, "delete %d", &row.Id)
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
		table.Insert(
			bptree_disk.Key(stmt.RowToInsert.Id),
			bptree_disk.Value(stmt.RowToInsert.ToBytes()),
		)
	case StatementDelete:
		table.bptree.Delete(bptree_disk.Key(stmt.RowToInsert.Id))
	case StatementSelect:
		iter := table.Iterator()
		entry := iter.Next()
		for entry != nil {
			row := NewSampleRowFromBytes(*entry.Val)
			fmt.Println(row)
			entry = iter.Next()
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

type Table struct {
	pager  *bptree_disk.Pager
	bptree *bptree_disk.BpTree
}

func CreateTable(filename string) error {
	return bptree_disk.NewPager(filename, uint16(SizeTotal))
}

func OpenTable(filename string) (*Table, error) {
	pager, err := bptree_disk.LoadPager(filename)
	if err != nil {
		return nil, err
	}
	return &Table{
		pager:  pager,
		bptree: bptree_disk.NewBpTree(pager),
	}, nil
}

func (t *Table) Rows() uint {
	return uint(t.bptree.Len())
}

func (t *Table) Insert(key bptree_disk.Key, val bptree_disk.Value) {
	t.bptree.Insert(key, val)
}

func (t *Table) GetRow(key bptree_disk.Key) *bptree_disk.Value {
	return t.bptree.Find(key)
}

func (t *Table) Iterator() bptree_disk.Iterator {
	return t.bptree.Iterator()
}

func (t *Table) Close() error {
	return t.pager.FlushAll()
}

func (t *Table) Len() int {
	return t.bptree.Len()
}

func (t *Table) Dump() {
	t.bptree.Print()
}

func (t *Table) Info() {
	fmt.Printf("internal order: %d\n", bptree_disk.InternalOrder)
	fmt.Printf("leaf order: %d\n", t.bptree.LeafOrder())
	fmt.Printf("num page: %d\n", t.pager.NumPage())
	fmt.Printf("free page list: %v\n", t.pager.FreePageList())
}
