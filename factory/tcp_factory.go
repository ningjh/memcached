package factory

import (
    "github.com/ningjh/memcached/common"
    "github.com/ningjh/memcached/config"

    "net"
    "bufio"
)

// ConnectionFactory a factory create connection
type ConnectionFactory struct {
    config *config.Config
}

// NewTcpConnect create a tcp connection
func (cf *ConnectionFactory) NewTcpConnect(addr string, i uint32) (conn *common.Conn, err error) {
    tcpConn, err := net.Dial("tcp", addr)

    if err == nil {
        conn = common.NewConn(tcpConn, bufio.NewReadWriter(bufio.NewReader(tcpConn), bufio.NewWriter(tcpConn)), cf.config, i)
    }

    return
}

// NewConnectionFactory create a connection factory
func NewConnectionFactory(c *config.Config) *ConnectionFactory {
    return &ConnectionFactory{c}
}