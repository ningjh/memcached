//execute 'go test -bench . memcached_client_bench_test.go'
package test

import (
	"github.com/ningjh/memcached"
	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"

	"fmt"
	"testing"
)

func BenchmarkMemcachedClient(b *testing.B) {
	var conf = &config.Config{
		Servers:   []string{"10.0.0.162:5000", "127.0.0.1:11211", "10.0.0.162:5006"},
		InitConns: 5,
	}

	var client, err = memcached.NewMemcachedClient4T(conf)
	if err != nil {
		fmt.Printf("%+v\n", err)
		return
	}

	for i := 0; i < b.N*1000000; i++ {
		e := &common.Element{
			Key:   fmt.Sprintf("%s_%d", "10.0.0.162", i),
			Value: []byte("memcached client test"),
		}
		client.Set(e)
	}
}
