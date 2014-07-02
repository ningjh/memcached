//execute 'go test -v pool_test.go'

package pool

import (
    "github.com/ningjh/memcached/pool"
    "github.com/ningjh/memcached/config"
    "github.com/ningjh/memcached/selector"
    "testing"
)

var servers []string = []string{"127.0.0.1:6060"}
var key string = "test_pool_key"

func createPool(t *testing.T) *pool.ConnectionPool {
	c := config.New()
    if c == nil {
    	t.Error("new config error.")
    }

    c.Servers = servers

    p, err := pool.New(c)
    if err != nil {
        t.Error("new pool error.")
    }

    t.Logf("%+v\n", p)

    return p.(*pool.ConnectionPool)
}

func TestNewPool(t *testing.T) {
    createPool(t)
}

func TestGet(t *testing.T) {
	p := createPool(t)

    index, err := selector.SelectServer(servers, key)
    if err != nil {
    	t.Error("get index error")
    }

    t.Logf("pool len = %d", len(p.Pools[index]))

    _, err = p.Get(key)
    if err != nil {
    	t.Error("get gonn error")
    }

    t.Logf("pool len = %d", len(p.Pools[index]))
}

func TestRelease(t *testing.T) {
    p := createPool(t)

    index, err := selector.SelectServer(servers, key)
    if err != nil {
    	t.Error("get index error")
    }

    conn, _ := p.Get(key)
    conn1, _ := p.Get(key)

    t.Logf("pool len = %d", len(p.Pools[index]))


    p.Release(key, conn)
    t.Logf("pool len = %d", len(p.Pools[index]))

    p.Release(key, conn1)
    t.Logf("pool len = %d", len(p.Pools[index]))
}