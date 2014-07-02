//execute 'go test -v text_protocol_parse_test.go'

package parse

import (
    "testing"

    "github.com/ningjh/memcached/pool"
    "github.com/ningjh/memcached/config"
    "github.com/ningjh/memcached/parse"
)

func new() *parse.TextProtocolParse {
	c := config.New()
	c.Servers = []string{"127.0.0.1:11211"}

    p, _ := pool.New(c)

	return parse.NewTextProtocolParse(p, c)	
}

func TestSet(t *testing.T) {
    tpp := new()

    err := tpp.Store("set", "test1", 0, 0, 0, []byte("test1"))
    if err != nil {
    	t.Error(err)
    }
}

func TestSet2(t *testing.T) {
	tpp := new()

	err := tpp.Store("set", "test2", 1, 0, 0, []byte("5"))
	if err != nil {
		t.Error(err)
	}
}

func TestAdd(t *testing.T) {
	tpp := new()

	err := tpp.Store("add", "add1", 1, 20, 0, []byte("test3 haha"))
	if err != nil {
		t.Error(err)
	}
}

func TestReplace(t *testing.T) {
	tpp := new()

	err := tpp.Store("replace", "test1", 1, 0, 0, []byte("replace 3"))
	if err != nil {
		t.Error(err)
	}
}

func TestAppend(t *testing.T) {
	tpp := new()

	err := tpp.Store("append", "test1", 1, 0, 0, []byte("_append"))
	if err != nil {
		t.Error(err)
	}
}

func TestPrepend(t *testing.T) {
	tpp := new()

	err := tpp.Store("prepend", "test1", 1, 0, 0, []byte("prepend_"))
	if err != nil {
		t.Error(err)
	}
}

func TestCas(t *testing.T) {
	tpp := new()

	err := tpp.Store("cas", "test1", 1, 0, 0, []byte("prepend_2"))
	if err != nil {
		t.Error(err)
	}
}

func TestGet(t *testing.T) {
	tpp := new()

    var keys = []string{"add1", "test1", "test2", "abcdefg"}

	items := tpp.Retrieval("get", keys)

	for _, key := range keys {
        item, ok := items[key]

        if ok {
		    t.Logf("%+v", item)
		    t.Logf(string(item.Value()))
        }
	}
}

func TestGets(t *testing.T) {
	tpp := new()

    var keys = []string{"add1", "test1", "test2", "abcdefg"}

	items := tpp.Retrieval("gets", keys)

	for _, key := range keys {
        item, ok := items[key]

        if ok {
		    t.Logf("%+v", item)
		    t.Logf(string(item.Value()))
        }
	}
}

func TestDelete(t *testing.T) {
	tpp := new()

	if err := tpp.Deletion("test1"); err != nil {
		t.Error(err)
	}
}

func TestTouch(t *testing.T) {
	tpp := new()

    if err := tpp.Touch("test111", 5); err != nil {
    	t.Error(err)
    }
}

func TestIncrement(t *testing.T) {
	tpp := new()

    if value, err := tpp.IncrOrDecr("incr", "test2", 2); err != nil {
    	t.Error(err)
    } else {
    	t.Logf("%d", value)
    }
}

func TestDecrement(t *testing.T) {
	tpp := new()

    if value, err := tpp.IncrOrDecr("decr", "test23434", 1); err != nil {
    	t.Error(err)
    } else {
    	t.Logf("%d", value)
    }
}