package selector

import (
    "hash/crc32"
    "container/list"
    "fmt"
    "sync"

    "github.com/ningjh/memcached/config"
)

type Node struct {
	HashCode    uint32
	ServerIndex int
}

type Consistent struct {
    config           *config.Config
	circle           *list.List
	numberOfReplicas int
	sync.RWMutex
}

func NewConsistent(c *config.Config) *Consistent {
    return &Consistent{
        config           : c,
    	circle           : list.New(),
    	numberOfReplicas : c.NumberOfReplicas,
    }
}

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

func (c *Consistent) Add(key string) {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.numberOfReplicas; i++ {
        node := &Node{
        	HashCode      : c.hashCode(c.genKey(key, i)),
        	ServerIndex   : c.getServerIndex(key),
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

func (c *Consistent) Remove(key string) {
    c.Lock()
    defer c.Unlock()

    for i := 0; i < c.numberOfReplicas; i++ {
    	hashCode := c.hashCode(c.genKey(key, i))

    	for e := c.circle.Front(); e != nil; e = e.Next() {
    		if n, ok := e.Value.(*Node); ok {
    			if n.HashCode == hashCode {
    				c.circle.Remove(e)
    				break
    			}
    		}
    	}
    }
}

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
    	err = fmt.Errorf("Memcached: could not found a server")
    }

    return
}