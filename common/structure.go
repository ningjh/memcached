// Package common includes some general data structures
package common

// Item is a interface storage data return by get or gets command.
type Item interface {
    Key()   string
    Value() []byte
    Cas()   uint64
    Flags() uint32
}

// TextItem implements Item.
type TextItem struct {
	TKey   string
	TValue []byte
	TFlags uint32
	TCas   uint64
}

func (item *TextItem) Key() string {
	return item.TKey
}

func (item *TextItem) Value() []byte {
	return item.TValue
}

func (item *TextItem) Cas() uint64 {
	return item.TCas
}

func (item *TextItem) Flags() uint32 {
	return item.TFlags
}

// Element passed as a parameter to storage commands.
type Element struct {
	Key     string
	Flags   uint32
	Exptime int64    //seconds
	Cas     uint64
	Value   []byte
}