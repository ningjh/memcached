package factory

import (
	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"

	"net"
)

// ConnectionFactory a factory create connection
type ConnectionFactory struct {
	config *config.Config
}

// NewTcpConnect create a tcp connection
func (cf *ConnectionFactory) NewTcpConnect(addr string, i int) (conn *common.Conn, err error) {
	tcpConn, err := net.Dial("tcp", addr)

	if err == nil {
		conn = common.NewConn(tcpConn, cf.config, i)
	}

	return
}

// NewConnectionFactory create a connection factory
func NewConnectionFactory(c *config.Config) *ConnectionFactory {
	return &ConnectionFactory{c}
}
