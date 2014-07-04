package factory

import (
    "github.com/ningjh/memcached/common"

    "net"
    "bufio"
)

func NewTcpConnect(addr string) (conn *common.Conn, err error) {
	tcpConn, err := net.Dial("tcp", addr)

	if err == nil {
        conn = &common.Conn{
        	Conn : tcpConn,
        	RW   : bufio.NewReadWriter(bufio.NewReader(tcpConn), bufio.NewWriter(tcpConn)),
        }
	}

	return
}