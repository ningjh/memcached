//Package selector determine which memcached server to talk to
package selector

import (
    "hash/crc32"
    "errors"
)

func SelectServer(servers []string, key string) (index uint32, err error) {
    defer func(){
        if r := recover(); r != nil {
            switch t := r.(type) {
                case error  :
                    err = t
                case string :
                    err = errors.New(t)
                default :
                    err = errors.New("selector error: unknow runtime error")
            }
        }
    }()
    
    if(len(servers) == 0) {
        panic("selector error: servers must not empty")
    }

    if len(key) == 0 {
        panic("selector error: key must not empty")
    }

    hashCode := crc32.ChecksumIEEE([]byte(key))
    index = hashCode % uint32(len(servers))

    return
}