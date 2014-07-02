package test

import (
    "github.com/ningjh/memcached"
    "github.com/ningjh/memcached/common"

    "testing"
)

var clientT, _ = memcached.NewMemcachedClient4T("127.0.0.1:11211")

func TestSet(t *testing.T) {
	e := &common.Element{
		Key   : "test1",
		Value : []byte("ningjiahong"),
	}
 
	e2 := &common.Element{
		Key   : "test2",
		Value : []byte("10"),
	}

	if err := clientT.Set(e); err != nil {
		t.Errorf("%s", err)
	}

	if err := clientT.Set(e2); err != nil {
		t.Errorf("%s", err)
	}
}

func TestAdd(t *testing.T) {
	e := &common.Element{
		Key   : "test4",
		Value : []byte("ningjiahong"),
	}

	if err := clientT.Add(e); err != nil {
		t.Errorf("%s", err)
	}
}

func TestReplace(t *testing.T) {
	e := &common.Element{
		Key   : "test1",
		Value : []byte("ningjiahong123"),
	}

	if err := clientT.Replace(e); err != nil {
		t.Errorf("%s", err)
	}
}

func TestAppend(t *testing.T) {
	e := &common.Element{
		Key   : "test1",
		Value : []byte("_Append"),
	}

	if err := clientT.Append(e); err != nil {
		t.Errorf("%s", err)
	}
}

func TestPrepend(t *testing.T) {
	e := &common.Element{
		Key   : "test1",
		Value : []byte("Prepend_"),
	}

	if err := clientT.Prepend(e); err != nil {
		t.Errorf("%s", err)
	}
}

func TestCas(t *testing.T) {
    item := clientT.Gets("test1")

	e := &common.Element{
		Key   : "test1",
		Value : []byte("cas"),
		Cas   : item.Cas(),
	}

	if err := clientT.Cas(e); err != nil {
		t.Errorf("%s", err)
	}
}

func TestGet(t *testing.T) {
    if item := clientT.Get("test1"); item != nil {
    	t.Logf("%+v", item)
    } else {
    	t.Errorf("no value")
    }
}

func TestGetArray(t *testing.T) {
    if items := clientT.GetArray([]string{"test1", "test2"}); items != nil {
    	for _, v := range items {
    		t.Logf("%+v", v)
    	}
    } else {
    	t.Errorf("no value")
    }
}

func TestGets(t *testing.T) {
    if item := clientT.Gets("test1"); item != nil {
    	t.Logf("%+v", item)
    } else {
    	t.Errorf("no value")
    }
}

func TestGetsArray(t *testing.T) {
    if items := clientT.GetsArray([]string{"test1", "test2"}); items != nil {
    	for _, v := range items {
    		t.Logf("%+v", v)
    	}
    } else {
    	t.Errorf("no value")
    }
}

func TestDelete(t *testing.T) {
    if err := clientT.Delete("test1"); err != nil {
    	t.Errorf("%s", err)
    }
}

func TestIncr(t *testing.T) {
    if v, err := clientT.Incr("test2", 5); err != nil {
    	t.Errorf("%s", err)
    } else {
    	t.Logf("%d", v)
    }
}

func TestDecr(t *testing.T) {
    if v, err := clientT.Decr("test2", 2); err != nil {
    	t.Errorf("%s", err)
    } else {
    	t.Logf("%d", v)
    }
}

func TestTouch(t *testing.T) {
    if err := clientT.Touch("test2", 500); err != nil {
    	t.Errorf("%s", err)
    }
}