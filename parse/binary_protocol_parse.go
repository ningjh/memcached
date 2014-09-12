package parse

import (
	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"
	"github.com/ningjh/memcached/pool"

	"errors"
)

const (
	// header fields byte length
	headerLen    int = 24
	magicLen     int = 1
	opcodeLen    int = 1
	keyLen       int = 2
	extrasLen    int = 1
	dataTypeLen  int = 1
	statusLen    int = 2
	totalBodyLen int = 4
	opaqueLen    int = 4
	casLen       int = 8

	// command opcodes
	Get       uint8 = 0x00
	Set       uint8 = 0x01
	Add       uint8 = 0x02
	Replace   uint8 = 0x03
	Delete    uint8 = 0x04
	Increment uint8 = 0x05
	Decrement uint8 = 0x06
	GetQ      uint8 = 0x09
	GetK      uint8 = 0x0c
	GetKQ     uint8 = 0x0d
	Append    uint8 = 0x0e
	Prepend   uint8 = 0x0f
	Touch     uint8 = 0x1c

	// data types
	DataType  uint8 = 0x00

	// magic byte
	reqMagic uint8 = 0x80
	resMagic uint8 = 0x81
)

// General format of a packet
type packet struct {
	magic           uint8
	opcode          uint8
	keyLength       uint16
	extrasLength    uint8
	dataType        uint8
	statusOrVbucket uint16
	totalBodyLength uint32
	opaque          uint32
	cas             uint64
	extras          []byte
	key             []byte
	value           []byte
}

type BinaryPorotolParse struct {
	pool   pool.Pool
	config *config.Config
}

func NewBinaryProtocolParse(p pool.Pool, c *config.Config) *BinaryPorotolParse {
	return &BinaryPorotolParse{pool: p, config: c}
}

func (parse *BinaryPorotolParse) release(conn *common.Conn, doClose bool) {
	if doClose {
		go conn.Close()
	} else {
		go parse.pool.Release(conn)
	}
}

// checkError if the status code of a response packet is no zero, return error.
func (parse *BinaryPorotolParse) checkError(status uint16) (err error) {
	switch status {
		case 0x0000 : err = nil
		case 0x0001 : err = errors.New("Memcached : Key not found")
		case 0x0002 : err = errors.New("Memcached : Key exists")
		case 0x0003 : err = errors.New("Memcached : Value too large")
		case 0x0004 : err = errors.New("Memcached : Invalid arguments")
		case 0x0005 : err = errors.New("Memcached : Item not stored")
		case 0x0006 : err = errors.New("Memcached : Incr/Decr on non-numeric value")
		case 0x0007 : err = errors.New("Memcached : The vbucket belongs to another server")
		case 0x0008 : err = errors.New("Memcached : Authentication error")
		case 0x0009 : err = errors.New("Memcached : Authentication continue")
		case 0x0081 : err = errors.New("Memcached : Unknown command")
		case 0x0082 : err = errors.New("Memcached : Out of memory")
		case 0x0083 : err = errors.New("Memcached : Not supported")
		case 0x0084 : err = errors.New("Memcached : Internal error")
		case 0x0085 : err = errors.New("Memcached : Busy")
		case 0x0086 : err = errors.New("Memcached : Temporary failure")
		default     : err = errors.New("Memcached : Unknow error")
	}

	return
}