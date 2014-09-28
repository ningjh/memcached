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

// fillPacket fill the send packet and write the packet to buffer.
func (parse *BinaryPorotolParse) fillPacket(p *packet, conn *common.Conn) (err error) {
	var header []byte = make([]byte, headerLen)
	var i      int

	// fill request header
	header[i] = p.magic;                                                     i += magicLen
	header[i] = p.opcode;                                                    i += opcodeLen
	binary.BigEndian.PutUint16(header[i:i+keyLen],       p.keyLength);       i += keyLen
	header[i] = p.extrasLength;                                              i += extrasLen
	header[i] = p.dataType;                                                  i += dataTypeLen
	binary.BigEndian.PutUint16(header[i:i+statusLen],    p.statusOrVbucket); i += statusLen
	binary.BigEndian.PutUint32(header[i:i+totalBodyLen], p.totalBodyLength); i += totalBodyLen
	binary.BigEndian.PutUint32(header[i:i+opaqueLen],    p.opaque);          i += opaqueLen
	binary.BigEndian.PutUint64(header[i:i+casLen],       p.cas)

	// write content to buffer
	_, err = conn.WriteToBuffer(header)
	_, err = conn.WriteToBuffer(p.extras)
	_, err = conn.WriteToBuffer(p.key)
	_, err = conn.WriteToBuffer(p.value)

	return
}

// parsePacket parse the response packet from serer.
func (parse *BinaryPorotolParse) parsePacket(conn *common.Conn) (p *packet, err error) {
	var header []byte = make([]byte, headerLen)
	var i, n   int

	// read response header
	if n, err = conn.Read(header); err != nil {
		return
	} else if n != headerLen {
		err = errors.New("Memcached : Unknow error")
		return
	}

	// parse response header
	p = &packet{}
	p.magic           = header[i];                                         i += magicLen
	p.opcode          = header[i];                                         i += opcodeLen
	p.keyLength       = binary.BigEndian.Uint16(header[i:i+keyLen]);       i += keyLen
	p.extrasLength    = header[i];                                         i += extrasLen
	p.dataType        = header[i];                                         i += dataTypeLen
	p.statusOrVbucket = binary.BigEndian.Uint16(header[i:i+statusLen]);    i += statusLen
	p.totalBodyLength = binary.BigEndian.Uint32(header[i:i+totalBodyLen]); i += totalBodyLen
	p.opaque          = binary.BigEndian.Uint32(header[i:i+opaqueLen]);    i += opaqueLen
	p.cas             = binary.BigEndian.Uint64(header[i:i+casLen])

	// read extras from response if exist
	if p.extrasLength > 0 {
		p.extras = make([]byte, p.extrasLength)

		if n, err = conn.Read(p.extras); err != nil {
			return
		} else if n != int(p.extrasLength) {
			err = errors.New("Memcached : Unknow error")
			return
		}
	}

	// read key from response if exist
	if p.keyLength > 0 {
		p.key = make([]byte, p.keyLength)

		if n, err = conn.Read(p.key); err != nil {
			return
		} else if n != int(p.keyLength) {
			err = errors.New("Memcached : Unknow error")
			return
		}
	}

	// read value from response if exist
	valueLength := int(p.totalBodyLength) - int(p.keyLength) - int(p.extrasLength)
	if valueLength > 0 {
		p.value = make([]byte, valueLength)

		if n, err = conn.Read(p.value); err != nil {
			return
		} else if n != valueLength {
			err = errors.New("Memcached : Unknow error")
			return
		}
	}

	return
}

// Retrieval retrieve data from server
func (parse *BinaryPorotolParse) Retrieval(keys []string) (items map[string]common.Item) {
	// result set of items
	items = make(map[string]common.Item)

	if len(keys) == 0 {
		return
	}

	keyMap := make(map[int][]string)

	// if a key has the same index, they will put together.
	for _, key := range keys {
		if index, err := parse.pool.GetNode(key); err != nil {
			return
		} else {
			// same index, same slice
			if ks, ok := keyMap[index]; ok {
				keyMap[index] = append(ks, key)
			} else {
				ks = make([]string, 0, 5)
				keyMap[index] = append(ks, key)
			}
		}
	}

	// send the get command line, and parse response
	LoopOut:
	for i, ks := range keyMap {
		// get connect by key
		conn, err := parse.pool.Get(ks[0])

		if err != nil || conn.Index != i {
			continue LoopOut
		}

		loopCount := len(ks) - 1
		
		for j := 0; j <= loopCount; j++ {
			reqPacket := &packet{
				magic          : reqMagic,
				keyLength      : uint16(len(ks[j])),
				key            : []byte(ks[j]),
				totalBodyLength: uint32(len(ks[j])),
			}

			//the first n-1 being getkq, the last being a regular getk
			if j < loopCount {
				reqPacket.opcode = GetKQ
			} else {
				reqPacket.opcode = GetK
			}

			if err := parse.fillPacket(reqPacket, conn); err != nil {
				parse.release(conn, true)
				continue LoopOut
			}
		}

		// send content to memcached server
		if err := conn.Flush(); err != nil {
			parse.release(conn, true)
			continue LoopOut
		}

		// receive response from memcached server
		for j := 0; j <= loopCount; j++ {
			resPacket, err := parse.parsePacket(conn)
			if err != nil {
				parse.release(conn, true)
				continue LoopOut
			}

			if err := parse.checkError(resPacket.statusOrVbucket); err != nil {
				continue
			}

			// fill item
			item := &common.BinaryItem{BCas:resPacket.cas}

			if resPacket.keyLength > 0 {
				item.BKey = string(resPacket.key)
			}

			if len(resPacket.value) > 0 {
				item.BValue = make([]byte, len(resPacket.value))
				copy(item.BValue, resPacket.value)
			}

			if resPacket.extrasLength > 0 {
				item.BFlags = binary.BigEndian.Uint32(resPacket.extras)
			}

			items[item.BKey] = item

			// if the item is the last one, done!
			if item.BKey == ks[loopCount] {
				break
			}
		}

		parse.release(conn, false)
	}

	return
}

func (parse *BinaryPorotolParse) requestAndResponse(conn *common.Conn, reqPacket *packet) (resPacket *packet, err error) {
	if err = parse.fillPacket(reqPacket, conn); err != nil {
		parse.release(conn, true)
		return
	}

	if err = conn.Flush(); err != nil {
		parse.release(conn, true)
		return
	}

	if resPacket, err = parse.parsePacket(conn); err != nil {
		parse.release(conn, true)
		return
	} else {
		err = parse.checkError(resPacket.statusOrVbucket)
		parse.release(conn, false)
	}

	return
}

// Set, Add, Replace
func (parse *BinaryPorotolParse) Store(opr uint8, key string, flags uint32, exptime uint32, cas uint64, value []byte) (err error) {
	conn, err := parse.pool.Get(key)
	if err != nil {
		return
	}

	reqPacket := &packet{
		magic       : reqMagic,
		opcode      : opr,
		keyLength   : uint16(len(key)),
		extrasLength: 8,
		cas         : cas,
		key         : []byte(key),
		value       : value,
	}
	reqPacket.totalBodyLength = uint32(reqPacket.keyLength) + uint32(reqPacket.extrasLength) + uint32(len(reqPacket.value))
	reqPacket.extras          = make([]byte, reqPacket.extrasLength)
	binary.BigEndian.PutUint32(reqPacket.extras[:4], flags)
	binary.BigEndian.PutUint32(reqPacket.extras[4:], exptime)

	_, err = parse.requestAndResponse(conn, reqPacket)

	return
}

func (parse *BinaryPorotolParse) Deletion(key string) (err error) {
	conn, err := parse.pool.Get(key)
	if err != nil {
		return
	}

	reqPacket := &packet{
		magic           : reqMagic,
		opcode          : Delete,
		keyLength       : uint16(len(key)),
		totalBodyLength : uint32(len(key)),
		key             : []byte(key),
	}

	_, err = parse.requestAndResponse(conn, reqPacket)

	return
}

func (parse *BinaryPorotolParse) IncrOrDecr(opr uint8, key string, value uint64, exptime uint32) (v uint64, err error) {
	conn, err := parse.pool.Get(key)
	if err != nil {
		return
	}

	reqPacket := &packet{
		magic : reqMagic,
		opcode : opr,
		keyLength : uint16(len(key)),
		extrasLength : 20,
		key : []byte(key),
	}
	reqPacket.totalBodyLength = uint32(reqPacket.keyLength) + uint32(reqPacket.extrasLength) + uint32(len(reqPacket.value))
	reqPacket.extras          = make([]byte, reqPacket.extrasLength)
	binary.BigEndian.PutUint64(reqPacket.extras[:8],   value)
	binary.BigEndian.PutUint64(reqPacket.extras[8:16], 0)
	binary.BigEndian.PutUint32(reqPacket.extras[16:],  exptime)

	resPacket, err := parse.requestAndResponse(conn, reqPacket)
	if err == nil {
		v = binary.BigEndian.Uint64(resPacket.value)
	}

	return
}

func (parse *BinaryPorotolParse) AppendOrPrepend(opr uint8, key string, value []byte) (err error) {
	conn, err := parse.pool.Get(key)
	if err != nil {
		return
	}

	reqPacket := &packet{
		magic       : reqMagic,
		opcode      : opr,
		keyLength   : uint16(len(key)),
		key         : []byte(key),
		value       : value,
	}
	reqPacket.totalBodyLength = uint32(reqPacket.keyLength) + uint32(reqPacket.extrasLength) + uint32(len(reqPacket.value))

	_, err = parse.requestAndResponse(conn, reqPacket)

	return
}

func (parse *BinaryPorotolParse) Touch(key string, exptime uint32) (err error) {
	conn, err := parse.pool.Get(key)
	if err != nil {
		return
	}

	reqPacket := &packet{
		magic        : reqMagic,
		opcode       : Touch,
		keyLength    : uint16(len(key)),
		extrasLength : 4,
		key          : []byte(key),
	}
	reqPacket.totalBodyLength = uint32(reqPacket.keyLength) + uint32(reqPacket.extrasLength) + uint32(len(reqPacket.value))
	reqPacket.extras          = make([]byte, reqPacket.extrasLength)
	binary.BigEndian.PutUint32(reqPacket.extras, exptime)

	_, err = parse.requestAndResponse(conn, reqPacket)

	return
}