//execute 'go test -v consistent_hashing_test.go'
package selector

import (
    "testing"
    "fmt"

    "github.com/ningjh/memcached/selector"
    "github.com/ningjh/memcached/config"
)

var servers []string = []string{"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089"}

func TestAdd(t *testing.T) {
    conf := config.New()
    conf.Servers = servers

    consistent := selector.NewConsistent(conf)

    for _, server := range servers {
        consistent.Add(server)
    }

    consistent.Print()
}

func TestGet(t *testing.T) {
	conf := config.New()
    conf.Servers = servers

    consistent := selector.NewConsistent(conf)

    for _, server := range servers {
        consistent.Add(server)
    }

    for i := 0; i < 30; i++ {
        key := servers[i % len(servers)]
        t.Log(consistent.Get(fmt.Sprintf("%s-oef%d", key, i)))
    }
}