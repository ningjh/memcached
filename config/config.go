package config

// Config the connection pool configuration.
type Config struct {
    Servers      []string    //memcached servers
    InitConns    uint16      //connect pool size of each server
    ReadTimeout  int64       //Millisecond
    WriteTimeout int64       //Millisecond
    Rehash       bool        //true: if a Memcached server crash, choose another one; false: just return an error
}

func New() *Config {
    return &Config{
        InitConns : 10,
    }
}