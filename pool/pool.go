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
    GetByIndex(int) (*common.Conn, error)
    Release(*common.Conn)
    GetNode(string) (int, error)
}

type ConnectionPool struct {
    pools      []chan *common.Conn
    config     *config.Config
    factory    *factory.ConnectionFactory
    consistent *selector.Consistent
}

// return a ConnectionPool instance, and for each server initializes a connection pool
func New(config *config.Config) (Pool, error) {
    pool := &ConnectionPool{
        pools      : make([]chan *common.Conn, 0, len(config.Servers)),
        config     : config,
        factory    : factory.NewConnectionFactory(config),
        consistent : selector.NewConsistent(config),
    }

    for i := 0; i < len(pool.config.Servers); i++ {
        pool.pools = append(pool.pools, make(chan *common.Conn, pool.config.InitConns))

        for j := 0; j < int(pool.config.InitConns); j++ {
            conn, err := pool.factory.NewTcpConnect(pool.config.Servers[i], i)

            if err != nil {
                return nil, err
            } else {
                pool.pools[i] <- conn
            }
        }

        pool.consistent.Add(pool.config.Servers[i])
    }

    pool.consistent.RefreshTicker()

    return pool, nil
}

// GetNode get consistent hashing node
func (pool *ConnectionPool) GetNode(key string) (int, error) {
    return pool.consistent.Get(key)
}

// GetByIndex get connect with key's index
func (pool *ConnectionPool) GetByIndex(i int) (conn *common.Conn, err error) {
    if i < 0 || i >= len(pool.pools)) {
        err = errors.New("Memcached : index out of range")
        return
    }

    for {
        conn = pool.get(i)
        
        if conn.Connected() {
            break
        } else {
            pool.Release(conn)

            if conn, err = pool.factory.NewTcpConnect(pool.config.Servers[i], i); err != nil {
                pool.consistent.Remove(pool.config.Servers[i])
                
                if pool.consistent.Len() == 0 {
                    break
                }
            } else {
                break
            }
        }
    }

    return
}

// Get get connect with key
func (pool *ConnectionPool) Get(key string) (*common.Conn, error) {
    i, err := pool.GetNode(key)

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

func (pool *ConnectionPool) get(i int) (*common.Conn) {
    select {
        case conn := <- pool.pools[i] :
            return conn
    }
}

func (pool *ConnectionPool) release(i int, conn *common.Conn) {
    select {
        case pool.pools[i] <- conn :
        default :
            conn.Close()
    }
}