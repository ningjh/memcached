//execute 'go test -v tcp_factory_test.go'

package factory

import (
    "memcached/factory"
    "testing"
)

var servers []string = []string{"127.0.0.1:6060"}

func TestNewTcpConnection(t *testing.T) {
    conn, err := factory.NewTcpConnection(servers[0])

    if err != nil {
    	t.Error(err)
    } else {
    	t.Log(conn)
    }

    err = factory.CloseTcpConnection(conn)
    if err != nil {
    	t.Error(err)
    }
}