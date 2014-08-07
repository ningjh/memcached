//execute 'go test -bench . consistent_hashing_bench_test.go'

package selector

import(
    "testing"
    "fmt"
    "github.com/ningjh/memcached/selector"
    "github.com/ningjh/memcached/config"
)

var servers []string = []string{"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089"}

func BenchmarkConsistentAdd(b *testing.B) {
    conf := config.New()
    conf.Servers = servers

    consistent := selector.NewConsistent(conf)

    for i := 0; i < 300; i++ {
        key := fmt.Sprintf("127.0.0.1_%d", i)
        consistent.Add(key)
    }
}

func BenchmarkConsistentGet(b *testing.B) {
    conf := config.New()
    conf.Servers = servers

    consistent := selector.NewConsistent(conf)

	for _, v := range servers {
        consistent.Add(v)
	}

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("127.0.0.1_%d", i)
		b.Log(consistent.Get(key))
	}
}