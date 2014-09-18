// Package memcached provides a client for memcached.
package memcached

import (
	"fmt"

	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"
	"github.com/ningjh/memcached/parse"
	"github.com/ningjh/memcached/pool"
)

// MemcachedClient4T implements the text protocol.
type MemcachedClient4T struct {
	parse *parse.TextProtocolParse
}

// NewMemcachedClient4T return a client that implements the text protocol.
func NewMemcachedClient4T(c *config.Config) (*MemcachedClient4T, error) {
	if len(c.Servers) == 0 {
		return nil, fmt.Errorf("Memcached : Servers must not empty")
	}

	if c.InitConns <= 0 {
		c.InitConns = 15
	}

	if c.NumberOfReplicas <= 0 {
		c.NumberOfReplicas = 20
	}

	if c.RefreshHashIntervalInSecond <= 0 {
		c.RefreshHashIntervalInSecond = 10
	}

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
		return fmt.Errorf("Memcached : nil pointer error")
	}

	return client.parse.Store(opr, e.Key, e.Flags, e.Exptime, e.Cas, e.Value)
}

// Set store this data
func (client *MemcachedClient4T) Set(e *common.Element) error {
	return client.store("set", e)
}

//Add store this data, but only if the server doesn't already hold data for this key
func (client *MemcachedClient4T) Add(e *common.Element) error {
	return client.store("add", e)
}

// Replace store this data, but only if the server does already hold data for this key
func (client *MemcachedClient4T) Replace(e *common.Element) error {
	return client.store("replace", e)
}

// Append add this data to an existing key after existing data
func (client *MemcachedClient4T) Append(e *common.Element) error {
	return client.store("append", e)
}

// Prepend add this data to an existing key before existing data
func (client *MemcachedClient4T) Prepend(e *common.Element) error {
	return client.store("prepend", e)
}

// Cas store this data but only if no one else has updated since I last fetched it
func (client *MemcachedClient4T) Cas(e *common.Element) error {
	return client.store("cas", e)
}

// Get retrieval data with this key
func (client *MemcachedClient4T) Get(key string) (item common.Item, err error) {
	items, err := client.parse.Retrieval("get", []string{key})

	if err == nil {
		var ok bool
		if item, ok = items[key]; !ok {
			err = fmt.Errorf("Memcached : no data error")
		}
	}

	return
}

// GetArray retrieval datas with keys
func (client *MemcachedClient4T) GetArray(keys []string) (items map[string]common.Item, err error) {
	items, err = client.parse.Retrieval("get", keys)

	if err == nil {
		if len(items) == 0 {
			err = fmt.Errorf("Memcached : no data error")
			items = nil
		}
	} else {
		items = nil
	}

	return
}

// Gets retrieval data with this key, include the 'cas' field
func (client *MemcachedClient4T) Gets(key string) (item common.Item, err error) {
	items, err := client.parse.Retrieval("gets", []string{key})

	if err == nil {
		var ok bool
		if item, ok = items[key]; !ok {
			err = fmt.Errorf("Memcached : no data error")
		}
	}

	return
}

// GetsArray retrieval datas with keys, include the 'cas' field
func (client *MemcachedClient4T) GetsArray(keys []string) (items map[string]common.Item, err error) {
	items, err = client.parse.Retrieval("gets", keys)

	if err == nil {
		if len(items) == 0 {
			err = fmt.Errorf("Memcached : no data error")
			items = nil
		}
	} else {
		items = nil
	}

	return
}

// Delete delete data with this key
func (client *MemcachedClient4T) Delete(key string) error {
	return client.parse.Deletion(key)
}

// Incr change data for some item in-place, incrementing it
func (client *MemcachedClient4T) Incr(key string, value uint64) (uint64, error) {
	return client.parse.IncrOrDecr("incr", key, value)
}

// Decr change data for some item in-place, decrementing it
func (client *MemcachedClient4T) Decr(key string, value uint64) (uint64, error) {
	return client.parse.IncrOrDecr("decr", key, value)
}

// Touch update the expiration time of an existing item without fetching it
func (client *MemcachedClient4T) Touch(key string, exptime uint32) error {
	return client.parse.Touch(key, exptime)
}
