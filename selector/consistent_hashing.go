//Package selector determine which memcached server to talk to
package selector

import (
	"container/list"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/ningjh/memcached/config"
	"github.com/ningjh/memcached/factory"
)

// Node server virtual node
type Node struct {
	HashCode    uint32
	ServerIndex int
}

// Consistent consistent hashing table
type Consistent struct {
	config           *config.Config
	circle           *list.List    //store virtual nodes
	numberOfReplicas int
	nodesStatus      []bool        //the memcached server status, enabled or crash
	factory          *factory.ConnectionFactory
	sync.RWMutex
}

func NewConsistent(c *config.Config) *Consistent {
	return &Consistent{
		config:           c,
		circle:           list.New(),
		numberOfReplicas: c.NumberOfReplicas,
		factory:          factory.NewConnectionFactory(c),
		nodesStatus:      make([]bool, len(c.Servers)),
	}
}

// genKey generate key for a virtual node
func (c *Consistent) genKey(key string, i int) string {
	return fmt.Sprintf("%s#%d", key, i)
}

func (c *Consistent) hashCode(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) getServerIndex(key string) int {
	for i, v := range c.config.Servers {
		if v == key {
			return i
		}
	}

	return 0
}

// add store a virtual node
func (c *Consistent) add(key string) {
	serverIndex := c.getServerIndex(key)
	c.nodesStatus[serverIndex] = true

	for i := 0; i < c.numberOfReplicas; i++ {
		node := &Node{
			HashCode:    c.hashCode(c.genKey(key, i)),
			ServerIndex: serverIndex,
		}

		if e := c.circle.Back(); e == nil {
			//if the circle is empty, insert the node at the front of the circle.
			c.circle.PushFront(node)
		} else {
			if n, ok := e.Value.(*Node); ok {
				if node.HashCode > n.HashCode {
					//if the node is the maximal, insert the node at the back of the circle.
					c.circle.PushBack(node)
				} else {
					for e = c.circle.Front(); e != nil; e = e.Next() {
						if n, ok = e.Value.(*Node); ok {
							if node.HashCode < n.HashCode {
								c.circle.InsertBefore(node, e)
								break
							}
						}
					}
				}
			}
		}
	}
}

// Add store a virtual node
func (c *Consistent) Add(key string) {
	c.Lock()
	defer c.Unlock()

	c.add(key)
}

// Remove remove a virtual node
func (c *Consistent) Remove(key string) {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.numberOfReplicas; i++ {
		hashCode := c.hashCode(c.genKey(key, i))

		for e := c.circle.Front(); e != nil; e = e.Next() {
			if n, ok := e.Value.(*Node); ok {
				if n.HashCode == hashCode {
					c.circle.Remove(e)
					c.nodesStatus[n.ServerIndex] = false
					break
				}
			}
		}
	}
}

// Get retrieval a virtual node
func (c *Consistent) Get(key string) (server int, err error) {
	c.RLock()
	defer c.RUnlock()

	server = -1
	hashCode := c.hashCode(key)

	for e := c.circle.Front(); e != nil; e = e.Next() {
		if n, ok := e.Value.(*Node); ok {
			if hashCode < n.HashCode {
				server = n.ServerIndex
				break
			}
		}
	}

	if server == -1 {
		if e := c.circle.Front(); e != nil {
			if n, ok := e.Value.(*Node); ok {
				server = n.ServerIndex
			}
		}
	}

	if server == -1 {
		err = fmt.Errorf("Memcached : could not found a server")
	}

	return
}

// RefreshTicker the background task regularly. 
// Add a memcached server into hash table when it has recovered from a panic
func (c *Consistent) RefreshTicker() {
	ticker := time.NewTicker(time.Second * time.Duration(c.config.RefreshHashIntervalInSecond))

	go func() {
		for _ = range ticker.C {
			c.Lock()

			for i, v := range c.config.Servers {
				if !c.nodesStatus[i] {
					if conn, err := c.factory.NewTcpConnect(v, i); err == nil {
						if conn.Connected() {
							c.add(v)
						}
						conn.Close()
					}
				}
			}

			c.Unlock()
		}
	}()
}
