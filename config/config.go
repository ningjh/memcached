package config

import (
    "time"
)

type Config struct {
	Servers      []string    //memcached servers
	InitConns    uint16      //connect pool size of each server
	ReadTimeout  time.Time
	WriteTimeout time.Time
}

func New() *Config {
    return &Config{
    	InitConns : 10,
    }
}