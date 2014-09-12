package common

import (
	"github.com/ningjh/memcached/config"

	"bufio"
	"net"
	"time"
)

// Conn wrap a net.Conn, and provide a buffer reader and writer
type Conn struct {
	Conn   net.Conn
	RW     *bufio.ReadWriter
	config *config.Config
	Index  int
}

func NewConn(conn net.Conn, c *config.Config, i int) *Conn {
	return &Conn{
		Conn:   conn,
		RW:     bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		config: c,
		Index:  i,
	}
}

// SetReadTimeout set the connect read timeout.
func (c *Conn) SetReadTimeout() {
	if c.config.ReadTimeout > 0 {
		c.Conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.config.ReadTimeout)))
	}
}

// SetWriteTimeout set the connect write timeout.
func (c *Conn) SetWriteTimeout() {
	if c.config.WriteTimeout > 0 {
		c.Conn.SetWriteDeadline(time.Now().Add(time.Millisecond * time.Duration(c.config.WriteTimeout)))
	}
}

// Write writes the contents of p into the buffer.
func (c *Conn) Write(p []byte) (n int, err error) {
	c.SetWriteTimeout()

	if n, err = c.RW.Write(p); err == nil {
		err = c.RW.Flush()
	}

	return
}

// Read reads data into p. It returns the number of bytes read into p.
func (c *Conn) Read(p []byte) (n int, err error) {
	c.SetReadTimeout()
	return c.RW.Read(p)
}

// ReadString reads until the first occurrence of delim in the input, returning a string containing the data up to and including the delimiter.
func (c *Conn) ReadString(delim byte) (string, error) {
	c.SetReadTimeout()
	return c.RW.ReadString(delim)
}

// ReadByte reads and returns a single byte. If no byte is available, returns an error.
func (c *Conn) ReadByte() (byte, error) {
	c.SetReadTimeout()
	return c.RW.ReadByte()
}

// Close close the connection and release memory.
func (c *Conn) Close() {
	c.Conn.Close()
	c.Conn = nil
	c.RW = nil
	c.config = nil
}

func (c *Conn) Connected() bool {
	if c.Conn == nil {
		return false
	}

	if _, err := c.Write([]byte("version\r\n")); err == nil {
		c.ReadString('\n')
		return true
	} else {
		return false
	}
}