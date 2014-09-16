// Package pool implements a connection pool.
// Each server has a connection pool.
package pool

import (
	"github.com/ningjh/memcached/common"
	"github.com/ningjh/memcached/config"
	"github.com/ningjh/memcached/factory"
	"github.com/ningjh/memcached/selector"

	"errors"
)

type Pool interface {
	Get(string) (*common.Conn, error)
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
	if len(config.Servers) == 0 {
		return nil, errors.New("Memcached : Memcached Servers must not empty")
	}

	pool := &ConnectionPool{
		pools:      make([]chan *common.Conn, 0, len(config.Servers)),
		config:     config,
		factory:    factory.NewConnectionFactory(config),
		consistent: selector.NewConsistent(config),
	}

	for i := 0; i < len(pool.config.Servers); i++ {
		pool.pools = append(pool.pools, make(chan *common.Conn, pool.config.InitConns))

		for j := 0; j < int(pool.config.InitConns / 2 + 1); j++ {
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

// Get get connect with key
func (pool *ConnectionPool) Get(key string) (conn *common.Conn, err error) {
	var i, j int

	for j = 0; j < len(pool.config.Servers); j++ {
		if i, err = pool.GetNode(key); err == nil {
			if conn, err = pool.get(i); err == nil {
				if conn.Connected() {
					break
				} else {
					err = errors.New("Memcached : can not connect to Memcached server")
				}
			}

			if conn != nil {
				conn.Close()
			}
			
			pool.consistent.Remove(pool.config.Servers[i])

			// clean the pool
			pool.clean(i)
		} else {
			break
		}
	}

	return
}

// Release put connect back to the pool
func (pool *ConnectionPool) Release(conn *common.Conn) {
	if conn != nil {
		pool.release(conn.Index, conn)
	}
}

func (pool *ConnectionPool) get(i int) (*common.Conn, error) {
	select {
	case conn := <-pool.pools[i]:
		return conn, nil
	default:
		return pool.factory.NewTcpConnect(pool.config.Servers[i], i)
	}
}

func (pool *ConnectionPool) release(i int, conn *common.Conn) {
	select {
	case pool.pools[i] <- conn:
	default:
		conn.Close()
	}
}

func (pool *ConnectionPool) clean(i int) {
	for t := 0; t < int(pool.config.InitConns); t++ {
		if conn, err := pool.get(i); err == nil {
			conn.Close()
		} else {
			break
		}
	}
}