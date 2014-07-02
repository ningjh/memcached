//execute 'go test -bench . text_protocol_parse_bench_test.go'

package parse

import (
    "testing"
    "fmt"

    "memcached/pool"
    "memcached/config"
    "memcached/parse"
)

func new() *parse.TextProtocolParse {
	c := config.New()
	c.Servers = []string{"127.0.0.1:11211"}

    p, _ := pool.New(c)

	return parse.NewTextProtocolParse(p)	
}

//BenchmarkSet   50000    62343 ns/op
func BenchmarkSet(b *testing.B) {
    tpp := new()

	for i := 0; i < b.N; i++ {
        tpp.Store("set", fmt.Sprintf("test%d", i), uint32(i), 0, 0, []byte("fkjdfoie-=0987843/.,"))
	}
}