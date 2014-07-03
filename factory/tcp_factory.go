package factory

import (
    "github.com/ningjh/memcached/common"

    "net"
    "bufio"
)

func NewTcpConnect(addr string) (*common.Conn, error) {
	tcpConn, err := net.Dial("tcp", addr)

	if err == nil {
        conn := &common.Conn{
        	Conn : tcpConn,
        	RW   : bufio.NewReadWriter(bufio.NewReader(tcpConn), bufio.NewWriter(tcpConn)),
        }

        return conn, err
	}

	return nil, err
}