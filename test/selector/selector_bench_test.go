//execute 'go test -bench . selector_bench_test.go'

package selector

import(
    "testing"
    "github.com/ningjh/memcached/selector"
)

var servers []string = []string{"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
var key string = "asdfghjkloiuytrewqas12345678909876543212_-+=#$%@!~*/.<>?mkieasdfghjkloiuytrewqas123456768909876543212_-+=#$%@!~*/.<>?mkie2"

// BenchmarkSelectServer performance test
// test result:    5000000     717 ns/op
func BenchmarkSelectServer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		selector.SelectServer(servers, key)
	}
}