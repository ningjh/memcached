//execute 'go test -v selector_test.go'

package selector

import (
    "testing"
    "github.com/ningjh/memcached/selector"
)

var servers1 []string
var servers2 []string = []string{"127.0.0.1:8080", "127.0.0.1:8081"}
var servers3 []string = []string{"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}

var key1 string
var key2 string = "abc"
var key3 string = "asdfghjkloiuytrewqas12345678909876543212_-+=#$%@!~*/.<>?mkieasdfghjkloiuytrewqas123456768909876543212_-+=#$%@!~*/.<>?mkie2"
var key4 string = "jfeilsjf3534_fkdjofier-fskti#fjeiofe@fjde908"

// TestSelectServer1 empty servers test
func TestSelectServer1(t *testing.T) {
    _, err := selector.SelectServer(servers1, key2)
    if err != nil {
    	t.Errorf("error: %s", err.Error())
    }
}

// TestSelectServer2 empty key test
func TestSelectServer2(t *testing.T) {
    _, err := selector.SelectServer(servers2, key1)
    if err != nil {
    	t.Errorf("error: %s", err.Error())
    }
}

// TestSelectServer3 success test
func TestSelectServer3(t *testing.T) {
	server, err := selector.SelectServer(servers3, key3)
	if err != nil {
		t.Errorf("error: %s", err.Error())
	} else {
		t.Log("--- OK: TestSelectServer3")
		t.Log(server)
	}
}

