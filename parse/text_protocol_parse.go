// Package parse communicate with memcached server and parse the return data.
package parse

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"
	"github.com/ningjh/memcached/pool"
)

var (
	crlf            = "\r\n"
	lf         byte = '\n'
	whitespace      = " "
)

type TextProtocolParse struct {
	pool   pool.Pool
	config *config.Config
}

func NewTextProtocolParse(p pool.Pool, c *config.Config) *TextProtocolParse {
	return &TextProtocolParse{pool: p, config: c}
}

// Store ask the server to store some data identified by a key
func (parse *TextProtocolParse) Store(opr string, key string, flags uint32, exptime int64, cas uint64, value []byte) error {
	// get a connect from the pool
	conn, err := parse.pool.Get(key)
	if err != nil {
		return err
	}

	// create command
	command := parse.createCommand(opr, key, flags, exptime, cas, len(value))

	// merge all datas
	data := mergeBytes(command, value, []byte(crlf))

	// send datas to memcached server
	if _, err := conn.Write(data); err != nil {
		parse.release(conn, true)
		return err
	}

	// parse the response from server
	response, err := conn.ReadString(lf)
	if err != nil {
		parse.release(conn, true)
		return err
	}

	err = parse.checkError(response)

	// put the connect back to the pool
	parse.release(conn, false)

	return err
}

// Retrieval retrieve data from server
func (parse *TextProtocolParse) Retrieval(opr string, keys []string) (items map[string]common.Item, err error) {
	// result set
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
			return items, err
		}

		// same index, same slice
		if ks, ok := keyMap[index]; ok {
			keyMap[index] = append(ks, key)
		} else {
			ks = make([]string, 0, 5)
			keyMap[index] = append(ks, key)
		}
	}

	// send the get or gets command line, and parse response
	for i, ks := range keyMap {
		// get connect by key
		conn, err := parse.pool.Get(ks[0])
		if err != nil {
			return items, err
		} else {
			if conn.Index != i {
				return items, fmt.Errorf("Memcached : server nodes had been modified")
			}
		}

		// create command
		command := parse.createCommand(opr, strings.Join(ks, whitespace), nil, nil, nil, nil)

		// send datas to memcached server
		if _, err := conn.Write(command); err != nil {
			parse.release(conn, true)
			return items, err
		}

		// parse response
		for {
			if line, err := conn.ReadString(lf); err == nil {
				if err = parse.checkError(line); err != nil {
					parse.release(conn, true)
					return items, err
				}

				line = strings.Replace(line, crlf, "", -1)

				if line == "END" {
					break
				} else {
					params := strings.Split(line, whitespace)
					item := new(common.TextItem)

					item.TKey = params[1]

					if flags, err := strconv.ParseUint(params[2], 10, 32); err == nil {
						item.TFlags = uint32(flags)
					}

					// read value
					if dataLen, err := strconv.ParseUint(params[3], 10, 64); err == nil {
						for k := uint64(0); k < dataLen; k++ {
							if c, err := conn.ReadByte(); err == nil {
								item.TValue = append(item.TValue, c)
							} else {
								parse.release(conn, true)
								return items, err
							}
						}

						if _, err = conn.ReadByte(); err != nil { //read '\r'
							parse.release(conn, true)
							return items, err
						}

						if _, err = conn.ReadByte(); err != nil { //read '\n'
							parse.release(conn, true)
							return items, err
						}
					}

					if len(params) == 5 {
						if cas, err := strconv.ParseUint(params[4], 10, 64); err == nil {
							item.TCas = cas
						}
					}

					items[item.TKey] = item
				}
			} else {
				parse.release(conn, true)
				return items, err
			}
		}

		// put the connect back to the pool
		parse.release(conn, false)
	}

	return
}

// Deletion delete the item with key
func (parse *TextProtocolParse) Deletion(key string) error {
	// get a connect from the pool
	conn, err := parse.pool.Get(key)
	if err != nil {
		return err
	}

	// create command
	command := parse.createCommand("delete", key, nil, nil, nil, nil)

	// send datas to memcached server
	if _, err := conn.Write(command); err != nil {
		parse.release(conn, true)
		return err
	}

	// parse the response from server
	response, err := conn.ReadString(lf)
	if err != nil {
		parse.release(conn, true)
		return err
	}

	err = parse.checkError(response)

	// put the connect back to the pool
	parse.release(conn, false)

	return err
}

// IncrOrDecr increment or decrement an item, and return new value of the item's data
func (parse *TextProtocolParse) IncrOrDecr(opr string, key string, value uint64) (uint64, error) {
	// get a connect from the pool
	conn, err := parse.pool.Get(key)
	if err != nil {
		return 0, err
	}

	// create command
	command := parse.createCommand(opr, key, nil, nil, nil, value)

	// send datas to memcached server
	if _, err := conn.Write(command); err != nil {
		parse.release(conn, true)
		return 0, err
	}

	// parse the response from server
	response, err := conn.ReadString(lf)
	if err != nil {
		parse.release(conn, true)
		return 0, err
	}

	err = parse.checkError(response)

	// put the connect back to the pool
	parse.release(conn, false)

	if err == nil {
		return strconv.ParseUint(strings.Replace(response, crlf, "", -1), 10, 64)
	} else {
		return 0, err
	}
}

// Touch touch an item
func (parse *TextProtocolParse) Touch(key string, exptime int64) error {
	// get a connect from the pool
	conn, err := parse.pool.Get(key)
	if err != nil {
		return err
	}

	// create command
	command := parse.createCommand("touch", key, nil, exptime, nil, nil)

	// send datas to memcached server
	if _, err := conn.Write(command); err != nil {
		parse.release(conn, true)
		return err
	}

	// parse the response from server
	response, err := conn.ReadString(lf)
	if err != nil {
		parse.release(conn, true)
		return err
	}

	err = parse.checkError(response)

	// put the connect back to the pool
	parse.release(conn, false)

	return err
}

func (parse *TextProtocolParse) release(conn *common.Conn, doClose bool) {
	if doClose {
		go conn.Close()
	} else {
		go parse.pool.Release(conn)
	}
}

func (parse *TextProtocolParse) checkError(s string) (err error) {
	if len(strings.Trim(s, whitespace)) == 0 {
		err = fmt.Errorf("Memcached : empty value error")
		return
	}

	result := strings.Split(strings.Replace(s, crlf, "", -1), whitespace)

	switch result[0] {
	case "ERROR":
		err = fmt.Errorf("Memcached : nonexistent command name")
	case "CLIENT_ERROR":
		err = fmt.Errorf("Memcached : %s", result[1])
	case "SERVER_ERROR":
		err = fmt.Errorf("Memcached : %s", result[1])
	case "NOT_STORED":
		err = fmt.Errorf("Memcached : the command wasn't met")
	case "EXISTS":
		err = fmt.Errorf("Memcached : the item has been modified since you last fetched it")
	case "NOT_FOUND":
		err = fmt.Errorf("Memcached : the item did not exist")
	}

	return
}

func (parse *TextProtocolParse) createCommand(opr string, key, flags, exptime, cas, value interface{}) (command []byte) {
	switch opr {
	case "set", "add", "replace", "append", "prepend":
		command = []byte(fmt.Sprintf("%s %s %d %d %d %s", opr, key, flags, exptime, value, crlf))
	case "cas":
		command = []byte(fmt.Sprintf("%s %s %d %d %d %d %s", opr, key, flags, exptime, value, cas, crlf))
	case "get", "gets":
		command = []byte(fmt.Sprintf("%s %s %s", opr, key, crlf))
	case "delete":
		command = []byte(fmt.Sprintf("%s %s %s", opr, key, crlf))
	case "incr", "decr":
		command = []byte(fmt.Sprintf("%s %s %d %s", opr, key, value, crlf))
	case "touch":
		command = []byte(fmt.Sprintf("%s %s %d %s", opr, key, exptime, crlf))
	}

	return
}

func mergeBytes(bs ...[]byte) []byte {
	var l, n int = 0, 0

	for _, v := range bs {
		l += len(v)
	}

	var buffer = make([]byte, l)

	for _, v := range bs {
		n += copy(buffer[n:], v)
	}

	return buffer
}
