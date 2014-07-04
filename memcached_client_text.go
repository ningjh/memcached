// Package memcached provides a client for memcached.
package memcached

import (
    "fmt"

    "github.com/ningjh/memcached/pool"
    "github.com/ningjh/memcached/config"
    "github.com/ningjh/memcached/parse"
    "github.com/ningjh/memcached/common"
)

// MemcachedClient4T implements the text protocol.
type MemcachedClient4T struct {
	parse  *parse.TextProtocolParse
}

// NewMemcachedClient4T return a client that implements the text protocol.
func NewMemcachedClient4T(c *config.Config) (*MemcachedClient4T, error) {
	p, err := pool.New(c)
	if err != nil {
		return nil, err
	}

	tpp := parse.NewTextProtocolParse(p, c)

	return &MemcachedClient4T{tpp}, nil
}

// store ask the server to store some data identified by a key
func (client *MemcachedClient4T) store(opr string, e *common.Element) error {
	if e == nil {
        return fmt.Errorf("nil pointer error.")
	}

    return client.parse.Store(opr, e.Key, e.Flags, e.Exptime, e.Cas, e.Value)
}

func (client *MemcachedClient4T) Set(e *common.Element) error {
	return client.store("set", e)
}

func (client *MemcachedClient4T) Add(e *common.Element) error {
    return client.store("add", e)
}

func (client *MemcachedClient4T) Replace(e *common.Element) error {
    return client.store("replace", e)
}

func (client *MemcachedClient4T) Append(e *common.Element) error {
    return client.store("append", e)
}

func (client *MemcachedClient4T) Prepend(e *common.Element) error {
    return client.store("prepend", e)
}

func (client *MemcachedClient4T) Cas(e *common.Element) error {
    return client.store("cas", e)
}

func (client *MemcachedClient4T) Get(key string) (item common.Item, err error) {
    items, err := client.parse.Retrieval("get", []string{key})

    if err == nil {
        var ok bool
        if item, ok = items[key]; !ok {
            err = fmt.Errorf("no data error.")
        }
    }

    return
}

func (client *MemcachedClient4T) GetArray(keys []string) (items map[string]common.Item, err error) {
    items, err = client.parse.Retrieval("get", keys)

    if err == nil {
        if len(items) == 0 {
            err   = fmt.Errorf("no data error.")
            items = nil
        }
    } else {
        items = nil
    }

    return
}

func (client *MemcachedClient4T) Gets(key string) (item common.Item, err error) {
    items, err := client.parse.Retrieval("gets", []string{key})

    if err == nil {
        var ok bool
        if item, ok = items[key]; !ok {
            err = fmt.Errorf("no data error.")
        }
    }

    return
}

func (client *MemcachedClient4T) GetsArray(keys []string) (items map[string]common.Item, err error) {
    items, err = client.parse.Retrieval("gets", keys)

    if err == nil {
        if len(items) == 0 {
            err   = fmt.Errorf("no data error.")
            items = nil
        }
    } else {
        items = nil
    }

    return
}

func (client *MemcachedClient4T) Delete(key string) error {
    return client.parse.Deletion(key)
}

func (client *MemcachedClient4T) Incr(key string, value uint64) (uint64, error) {
    return client.parse.IncrOrDecr("incr", key, value)
}

func (client *MemcachedClient4T) Decr(key string, value uint64) (uint64, error) {
    return client.parse.IncrOrDecr("decr", key, value)
}

func (client *MemcachedClient4T) Touch(key string, exptime int64) error {
    return client.parse.Touch(key, exptime)
}