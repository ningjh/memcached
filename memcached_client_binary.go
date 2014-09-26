package memcached

import (
	"fmt"

	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"
	"github.com/ningjh/memcached/parse"
	"github.com/ningjh/memcached/pool"
)

// MemcachedClient4B implements the binary protocol.
type MemcachedClient4B struct {
	parse *parse.BinaryPorotolParse
}

// NewMemcachedClient4B return a client that implements the binary protocol.
func NewMemcachedClient4B(c *config.Config) (*MemcachedClient4B, error) {
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

	c.TextOrBinary = 1

	p, err := pool.New(c)
	if err != nil {
		return nil, err
	}

	tpp := parse.NewBinaryProtocolParse(p, c)

	return &MemcachedClient4B{tpp}, nil
}

// store ask the server to store some data identified by a key
func (client *MemcachedClient4B) store(opr uint8, e *common.Element) error {
	if e == nil {
		return fmt.Errorf("Memcached : nil pointer error")
	}

	return client.parse.Store(opr, e.Key, e.Flags, e.Exptime, e.Cas, e.Value)
}

// Set store this data
func (client *MemcachedClient4B) Set(e *common.Element) error {
	return client.store(parse.Set, e)
}

//Add store this data, but only if the server doesn't already hold data for this key
func (client *MemcachedClient4B) Add(e *common.Element) error {
	return client.store(parse.Add, e)
}

// Replace store this data, but only if the server does already hold data for this key
func (client *MemcachedClient4B) Replace(e *common.Element) error {
	return client.store(parse.Replace, e)
}

// Append add this data to an existing key after existing data
func (client *MemcachedClient4B) Append(e *common.Element) error {
	if e == nil {
		return fmt.Errorf("Memcached : nil pointer error")
	}
	return client.parse.AppendOrPrepend(parse.Append, e.Key, e.Value)
}

// Prepend add this data to an existing key before existing data
func (client *MemcachedClient4B) Prepend(e *common.Element) error {
	if e == nil {
		return fmt.Errorf("Memcached : nil pointer error")
	}
	return client.parse.AppendOrPrepend(parse.Prepend, e.Key, e.Value)
}

// Get retrieval data with this key
func (client *MemcachedClient4B) Get(key string) (item common.Item, err error) {
	items := client.parse.Retrieval([]string{key})

	var ok bool
	if item, ok = items[key]; !ok {
		err = fmt.Errorf("Memcached : no data error")
	}

	return
}

// GetArray retrieval datas with keys
func (client *MemcachedClient4B) GetArray(keys []string) (items map[string]common.Item, err error) {
	items = client.parse.Retrieval(keys)

	if len(items) == 0 {
		err = fmt.Errorf("Memcached : no data error")
		items = nil
	}

	return
}

// Delete delete data with this key
func (client *MemcachedClient4B) Delete(key string) error {
	return client.parse.Deletion(key)
}

// Incr change data for some item in-place, incrementing it
func (client *MemcachedClient4B) Incr(key string, value uint64) (uint64, error) {
	return client.parse.IncrOrDecr(parse.Increment, key, value, 0xffffffff)
}

// Decr change data for some item in-place, decrementing it
func (client *MemcachedClient4B) Decr(key string, value uint64) (uint64, error) {
	return client.parse.IncrOrDecr(parse.Decrement, key, value, 0xffffffff)
}

// Touch update the expiration time of an existing item without fetching it
func (client *MemcachedClient4B) Touch(key string, exptime uint32) error {
	return client.parse.Touch(key, exptime)
}
