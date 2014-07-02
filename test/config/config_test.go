//execute 'go test -v config_test.go'

package config

import (
    "github.com/ningjh/memcached/config"
    "testing"
)

func TestNewConfig(t *testing.T) {
	c := config.New()

	t.Logf("%+v\n", c)

	c.Servers   = []string{"127.0.0.1:8080", "192.168.2.156:8080"}
	c.InitConns = 25

	t.Logf("%+v\n", c)
}