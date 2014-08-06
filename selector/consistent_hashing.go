package selector

import (
    "hash/crc32"
    "container/list"
    "fmt"
    "sync"
)

type Node struct {
	HashCode uint32
	Server   string
}

type Consistent struct {
	circle           *list.List
	numberOfReplicas int
	sync.RWMutex
}

func NewConsistent(n int) *Consistent {
    return &Consistent{
    	circle           : list.New(),
    	numberOfReplicas : n,
    }
}

func (c *Consistent) genKey(key string, i int) string {
	return fmt.Sprintf("%s#%d", key, i)
}

func (c *Consistent) hashCode(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) Add(key string) {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.numberOfReplicas; i++ {
        node := &Node{
        	HashCode : c.hashCode(c.genKey(key, i)),
        	Server   : key,
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

func (c *Consistent) Get(key string) (server string, err error) {
    c.RLock()
    defer c.RUnlock()

    hashCode := c.hashCode(key)

    for e := c.circle.Front(); e != nil; e = e.Next() {
        if n, ok := e.Value.(*Node); ok {
        	if hashCode < n.HashCode {
        		server = n.Server
        		break
        	}
        }
    }

    if server == "" {
        if e := c.circle.Front(); e != nil {
        	if n, ok := e.Value.(*Node); ok {
        		server = n.Server

        	}
        }
    }

    if server == "" {
    	err = fmt.Errorf("Memcached: could not found a server")
    }

    return
}
