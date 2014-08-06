//execute 'go test -v consistent_hashing_test.go'
package selector

import (
    "testing"
    "time"
    "fmt"
    "github.com/ningjh/memcached/selector"
)

var servers []string = []string{"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089"}

func TestAdd(t *testing.T) {
    start := time.Now()

    consistent := selector.NewConsistent(100)

    for _, server := range servers {
        consistent.Add(server)
    }

    end := time.Now()
    t.Log(end.Nanosecond() - start.Nanosecond())
}

func TestGet(t *testing.T) {
	consistent := selector.NewConsistent(10)

    for _, server := range servers {
        consistent.Add(server)
    }

    key := "agcfd"
    for i := 0; i < 30; i++ {
    	if i == 15 {
            consistent.Remove("127.0.0.1:8080")
            consistent.Remove("127.0.0.1:8081")
            consistent.Remove("127.0.0.1:8082")
            consistent.Remove("127.0.0.1:8083")
            consistent.Remove("127.0.0.1:8086")
            consistent.Remove("127.0.0.1:8088")
    	}
        t.Log(consistent.Get(fmt.Sprintf("%s-%d", key, i)))
    }
}