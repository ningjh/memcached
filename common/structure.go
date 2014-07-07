// Package common includes some general data structures
package common

import (
    "github.com/ningjh/memcached/config"

    "bufio"
    "net"
    "time"
)

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

// Conn wrap a net.Conn, and provide a buffer reader and writer
type Conn struct {
	Conn   net.Conn
	RW     *bufio.ReadWriter
	config *config.Config
}

func NewConn(conn net.Conn, rw *bufio.ReadWriter, c *config.Config) *Conn {
	return &Conn{
		Conn   : conn,
		RW     : rw,
		config : c,
	}
}

func (c *Conn) Write(p []byte) (n int, err error) {
    c.SetWriteTimeout()

    if n, err = c.RW.Write(p); err == nil {
    	err = c.RW.Flush()
    }

    return
}

func (c *Conn) ReadString(delim byte) (string, error) {
    c.SetReadTimeout()
    return c.RW.ReadString(delim)
}

func (c *Conn) ReadByte() (byte, error) {
	c.SetReadTimeout()
	return c.RW.ReadByte()
}

func (c *Conn) Close() {
	c.Conn.Close()
	c.Conn   = nil
	c.RW     = nil
	c.config = nil
}

func (c *Conn) SetReadTimeout() {
	if c.config.ReadTimeout > 0 {
		c.Conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.config.ReadTimeout)))
	}
}

func (c *Conn) SetWriteTimeout() {
	if c.config.WriteTimeout > 0 {
		c.Conn.SetWriteDeadline(time.Now().Add(time.Millisecond * time.Duration(c.config.WriteTimeout)))
	}
}