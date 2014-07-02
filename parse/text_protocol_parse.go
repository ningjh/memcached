// Package parse communicate with memcached server and parse the return data.
package parse

import (
    "net"
    "fmt"
    "bufio"
    "bytes"
    "strings"
    "strconv"

    "memcached/factory"
    "memcached/pool"
    "memcached/common"
    "memcached/selector"
    "memcached/config"
)

var (
    crlf       = "\r\n"
    whitespace = " "
    stored     = []byte("STORED\r\n")
    end        = []byte("END\r\n")
    deleted    = []byte("DELETED\r\n")
    touched    = []byte("TOUCHED\r\n")
    notFound   = []byte("NOT_FOUND\r\n")
)

type TextProtocolParse struct {
	Pool   pool.Pool
    Config *config.Config
}

func NewTextProtocolParse(p pool.Pool, c *config.Config) *TextProtocolParse {
    return &TextProtocolParse {Pool : p, Config : c}
}

// Store ask the server to store some data identified by a key
func (parse *TextProtocolParse) Store(opr string, key string, flag uint32, exptime int64, cas uint64, value []byte) error {
    // get a connect from the pool
    conn, err := parse.getConn(key)
    if err != nil {
    	return err
    }

    rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

    var command []byte

    if opr == "cas" {
        command = []byte(fmt.Sprintf("%s %s %d %d %d %d %s", opr, key, flag, exptime, len(value), cas, crlf))
    } else {
        command = []byte(fmt.Sprintf("%s %s %d %d %d %s", opr, key, flag, exptime, len(value), crlf))
    }

    // send command line to server
    if _, err := rw.Write(command); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    // send the data to server
    if _, err := rw.Write(value); err != nil {
    	go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    // end with '\r\n'
    if _, err := rw.Write([]byte(crlf)); err != nil {
    	go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    if err := rw.Flush(); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }
    
    // parse the response from server
    response, err := rw.ReadSlice('\n')
    if err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    switch {
        case bytes.Equal(response, stored) :
            err = nil
        default :
            err = fmt.Errorf("%q", string(response[:len(response) - 2]))
    }

    // put the connect back to the pool
    go parse.releaseConn(key, conn)

    return err
}

// Retrieval retrieve data from server
func (parse *TextProtocolParse) Retrieval(opr string, keys []string) (items map[string]common.Item) {
    // result set
    items = make(map[string]common.Item)

    if len(keys) == 0 {
        return
    }

    keyMap := make(map[uint32][]string)

    // if a key has the same index, they will put together.
    for _, key := range keys {
        // calculate the key's index
        index, err := selector.SelectServer(parse.Config.Servers, key)

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

    // send the get or gets command line, and parse response
    for i, ks := range keyMap {
        // get connect by key's index
        conn, err := parse.getConnByIndex(i)
        if err != nil {
            return
        }

        rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

        command := []byte(fmt.Sprintf("%s %s %s", opr, strings.Join(ks, whitespace), crlf))

        if _, err := rw.Write(command); err != nil {
            go parse.closeConn(conn)
            go parse.releaseConnByIndex(i, nil)
            return
        }

        if err := rw.Flush(); err != nil {
            go parse.closeConn(conn)
            go parse.releaseConnByIndex(i, nil)
            return
        }

        // parse response
        for {
            line, err := rw.ReadBytes('\n')
            if err == nil {
                if bytes.Equal(line, end) {
                    break
                } else {
                    params := strings.Split(string(line[:len(line) - 2]), whitespace)
                    item   := new(common.TextItem)

                    item.TKey = params[1]

                    if flags, err := strconv.ParseUint(params[2], 10, 32); err == nil {
                        item.TFlags = uint32(flags)                        
                    }

                    if dataLen, err := strconv.ParseUint(params[3], 10, 64); err == nil {
                        for i := uint64(0); i < dataLen; i++ {
                            if c, err := rw.ReadByte(); err == nil {
                                item.TValue = append(item.TValue, c)
                            } else {
                                return
                            }
                        }
                        rw.ReadByte()  //read '\r'
                        rw.ReadByte()  //read '\n'
                    }

                    if len(params) == 5 {
                        if cas, err := strconv.ParseUint(params[4], 10, 64); err == nil {
                            item.TCas = cas
                        }
                    }

                    items[item.TKey] = item
                }
            } else {
                break
            }
        }

        // put the connect back to the pool
        go parse.releaseConnByIndex(i, conn)
    }

    return
}

// Deletion delete the item with key
func (parse *TextProtocolParse) Deletion(key string) error {
    conn, err := parse.getConn(key)
    if err != nil {
        return err
    }

    rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

    var command []byte = []byte(fmt.Sprintf("delete %s %s", key, crlf))

    if _, err := rw.Write(command); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    if err := rw.Flush(); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    response, err := rw.ReadSlice('\n')
    if err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    switch {
        case bytes.Equal(response, deleted) :
            err = nil
        default :
            err = fmt.Errorf("%q", string(response[:len(response) - 2]))
    }

    go parse.releaseConn(key, conn)

    return err
}

// IncrOrDecr increment or decrement an item, and return new value of the item's data
func (parse *TextProtocolParse) IncrOrDecr(opr string, key string, value uint64) (uint64, error) {
    conn, err := parse.getConn(key)
    if err != nil {
        return 0, err
    }

    rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

    var command []byte = []byte(fmt.Sprintf("%s %s %d %s", opr, key, value, crlf))

    if _, err := rw.Write(command); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return 0, err
    }

    if err := rw.Flush(); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return 0, err
    }

    response, err := rw.ReadSlice('\n')
    if err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return 0, err
    }

    switch {
        case bytes.Equal(response, notFound) :
            err = fmt.Errorf("%q", string(response[:len(response) - 2]))
        default :
            err = nil
    }

    go parse.releaseConn(key, conn)

    if err == nil {
        return strconv.ParseUint(string(response[:len(response) - 2]), 10, 64)
    } else {
        return 0, err
    }
}

// Touch touch an item
func (parse *TextProtocolParse) Touch(key string, exptime int64) error {
    conn, err := parse.getConn(key)
    if err != nil {
        return err
    }

    rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

    var command []byte = []byte(fmt.Sprintf("touch %s %d %s", key, exptime, crlf))

    if _, err := rw.Write(command); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    if err := rw.Flush(); err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }
    
    response, err := rw.ReadSlice('\n')
    if err != nil {
        go parse.closeConn(conn)
        go parse.releaseConn(key, nil)
        return err
    }

    switch {
        case bytes.Equal(response, touched) :
            err = nil
        default :
            err = fmt.Errorf("%q", string(response[:len(response) - 2]))
    }

    go parse.releaseConn(key, conn)

    return err
}

func (parse *TextProtocolParse) closeConn(conn net.Conn) {
    factory.CloseTcpConnection(conn)
}

func (parse *TextProtocolParse) getConn(key string) (net.Conn, error) {
	return parse.Pool.Get(key)
}

func (parse *TextProtocolParse) getConnByIndex(i uint32) (net.Conn, error) {
    return parse.Pool.GetByIndex(i)
}

func (parse *TextProtocolParse) releaseConn(key string, conn net.Conn) {
    parse.Pool.Release(key, conn)
}

func (parse *TextProtocolParse) releaseConnByIndex(i uint32, conn net.Conn) {
    parse.Pool.ReleaseByIndex(i, conn)
}