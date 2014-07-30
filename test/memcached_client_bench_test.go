package test

import (
    "github.com/ningjh/memcached"
    "github.com/ningjh/memcached/common"
    "github.com/ningjh/memcached/config"

    "testing"
    "fmt"
)

func BenchmarkMemcachedClient(b *testing.B) {
	var conf = &config.Config{
	    Servers : []string{"10.0.0.162:5000", "10.0.0.162:5006", "127.0.0.1:11211"},
	    Rehash  : true,
	}

	var client, _ = memcached.NewMemcachedClient4T(conf)

	for i := 0; i < b.N * 1000; i++ {
		e := &common.Element{
            Key   : fmt.Sprintf("%s_%d", "test", i),
            Value : []byte("memcached client test"),
        }
		client.Set(e)
	}
}