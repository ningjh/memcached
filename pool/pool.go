// Package pool implements a connection pool.
// Each server has a connection pool.
package pool

import (
    "memcached/config"
    "memcached/selector"
    "memcached/factory"
    "net"
    "errors"
)

type Pool interface {
	Get(string) (net.Conn, error)
    GetByIndex(uint32) (net.Conn, error)
	Release(string, net.Conn)
    ReleaseByIndex(uint32, net.Conn)

}

type ConnectionPool struct {
	pools  []chan net.Conn
	config *config.Config
}

// return a ConnectionPool instance, and for each server initializes a connection pool
func New(config *config.Config) (Pool, error) {
    pool := &ConnectionPool{
        pools  : make([]chan net.Conn, 0, len(config.Servers)),
        config : config,
    }

    for i := 0; i < len(pool.config.Servers); i++ {
        pool.pools = append(pool.pools, make(chan net.Conn, pool.config.InitConns))

        for j := 0; j < int(pool.config.InitConns); j++ {
            conn, err := factory.NewTcpConnection(pool.config.Servers[i])

            if err != nil {
                return nil, err
            } else {
                pool.pools[i] <- conn
            }
        }
    }

    return pool, nil
}

// GetByIndex get connect with key's index
func (pool *ConnectionPool) GetByIndex(i uint32) (net.Conn, error) {
    if i < 0 || i >= uint32(len(pool.pools)) {
        return nil, errors.New("index out of range")
    }

    select {
        case conn := <- pool.pools[i] :
            return conn, nil
    }
}

// Get get connect with key
func (pool *ConnectionPool) Get(key string) (net.Conn, error) {
    i, err := selector.SelectServer(pool.config.Servers, key)

    if err != nil {
    	return nil, err
    }

    return pool.GetByIndex(i)
}

// ReleaseByIndex put connect back to the pool
func (pool *ConnectionPool) ReleaseByIndex(i uint32, conn net.Conn) {
    if i < 0 || i >= uint32(len(pool.pools)) {
        return
    }

    if conn == nil {
        conn, err := factory.NewTcpConnection(pool.config.Servers[i])

        if err == nil {
            pool.pools[i] <- conn
        }
    } else {
        pool.pools[i] <- conn
    }
}

// Release put connect back to the pool
func (pool *ConnectionPool) Release(key string, conn net.Conn) {
	i, err := selector.SelectServer(pool.config.Servers, key)

    if err == nil {
        pool.ReleaseByIndex(i, conn)
    }
}