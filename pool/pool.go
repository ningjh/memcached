// Package pool implements a connection pool.
// Each server has a connection pool.
package pool

import (
    "github.com/ningjh/memcached/config"
    "github.com/ningjh/memcached/selector"
    "github.com/ningjh/memcached/factory"
    "github.com/ningjh/memcached/common"
    
    "errors"
)

type Pool interface {
    Get(string) (*common.Conn, error)
    GetByIndex(uint32) (*common.Conn, error)
    Release(*common.Conn)
}

type ConnectionPool struct {
    pools   []chan *common.Conn
    config  *config.Config
    factory *factory.ConnectionFactory
}

// return a ConnectionPool instance, and for each server initializes a connection pool
func New(config *config.Config) (Pool, error) {
    pool := &ConnectionPool{
        pools   : make([]chan *common.Conn, 0, len(config.Servers)),
        config  : config,
        factory : factory.NewConnectionFactory(config),
    }

    for i := 0; i < len(pool.config.Servers); i++ {
        pool.pools = append(pool.pools, make(chan *common.Conn, pool.config.InitConns))

        for j := 0; j < int(pool.config.InitConns); j++ {
            conn, err := pool.factory.NewTcpConnect(pool.config.Servers[i], uint32(i))

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
func (pool *ConnectionPool) GetByIndex(i uint32) (*common.Conn, error) {
    if i < 0 || i >= uint32(len(pool.pools)) {
        return nil, errors.New("Memcached: index out of range")
    }

    conn, err := pool.get(i)

    //if the Memcached server crash, choose another one.
    if pool.config.Rehash {
        var ok bool = false

        if err == nil {
            ok = conn.Connected()
        }

        if !ok {
            for i = 0; i < uint32(len(pool.config.Servers)); i++ {
                if conn, err = pool.get(i); err == nil {
                    if conn.Connected() {
                        break
                    }
                }
            }
        }
    }

    return conn, err
}

// Get get connect with key
func (pool *ConnectionPool) Get(key string) (*common.Conn, error) {
    i, err := selector.SelectServer(pool.config.Servers, key)

    if err != nil {
        return nil, err
    }

    return pool.GetByIndex(i)
}

// Release put connect back to the pool
func (pool *ConnectionPool) Release(conn *common.Conn) {
    if conn != nil {
        pool.release(conn.Index, conn)
    }
}

func (pool *ConnectionPool) get(i uint32) (*common.Conn, error) {
    select {
        case conn := <- pool.pools[i] :
            return conn, nil
        default :
            return pool.factory.NewTcpConnect(pool.config.Servers[i], i)
    }
}

func (pool *ConnectionPool) release(i uint32, conn *common.Conn) {
    select {
        case pool.pools[i] <- conn :
        default :
            conn.Close()
    }
}