package parse

import (
	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"
	"github.com/ningjh/memcached/pool"

	"errors"
	"encoding/binary"
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

func (parse *BinaryPorotolParse) fillPacket(p *packet, conn *common.Conn) (err error) {
	var header []byte = make([]byte, headerLen)
    var i      int    = 0

    // fill request header
    header[i] = p.magic
    i += magicLen

    header[i] = p.opcode
    i += opcodeLen

    binary.BigEndian.PutUint16(header[i:i+keyLen], p.keyLength)
    i += keyLen

    header[i] = p.extrasLength
    i += extrasLen

    header[i] = p.dataType
    i += dataTypeLen

    binary.BigEndian.PutUint16(header[i:i+statusLen], p.statusOrVbucket)
    i += statusLen

    binary.BigEndian.PutUint32(header[i:i+totalBodyLen], p.totalBodyLength)
    i += totalBodyLen

    binary.BigEndian.PutUint32(header[i:i+opaqueLen], p.opaque)
    i += opaqueLen

    binary.BigEndian.PutUint64(header[i:i+casLen], p.cas)

    // write content to buffer
    _, err = conn.WriteToBuffer(header)
	_, err = conn.WriteToBuffer(p.extras)
	_, err = conn.WriteToBuffer(p.key)
	_, err = conn.WriteToBuffer(p.value)

    return
}

func (parse *BinaryPorotolParse) Retrieval(keys []string) (items map[string]common.Item, err error) {
	// result set of items
	items = make(map[string]common.Item)

	if len(keys) == 0 {
		return
	}

	keyMap := make(map[int][]string)

	// if a key has the same index, they will put together.
	for _, key := range keys {
		// calculate the key's index
		index, err := parse.pool.GetNode(key)

		if err != nil {
			return
		}

		// same index, same slice
		if ks, ok := keyMap[index]; ok {
			keyMap[index] = append(ks, key)
		} else {
			ks = make([]string, 0, 5)
			keyMap[index] = append(ks, key)
		}
	}

	// send the get command line, and parse response
	for i, ks := range keyMap {
		// get connect by key
		conn, err := parse.pool.Get(ks[0])
		if err != nil {
			return
		} else {
			if conn.Index != i {
				err = errors.New("Memcached : server nodes had been modified")
				return
			}
		}

		loopCount := len(ks) - 1
		
		for j := 0; j <= loopCount; j++ {
			reqPacket := &packet{
				magic:           reqMagic,
				keyLength:       uint16(len(k)),
				key:             []byte(k),
				totalBodyLength: uint32(len(k)),
			}

			if j < loopCount {
				reqPacket.opcode = GetKQ
			} else {
				reqPacket.opcode = GetK
			}

			if err = parse.fillPacket(reqPacket, conn); err != nil {
				return
			}
		}

		// send content to memcached server
		if err = conn.Flush(); err != nil {
			return
		}
	}
}