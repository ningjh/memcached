package factory

import (
    "net"
)

func NewTcpConnection(addr string) (net.Conn, error) {
    return net.Dial("tcp", addr)
}

func CloseTcpConnection(conn net.Conn) error {
	if conn != nil {
        return conn.Close()
    }

    return nil
}