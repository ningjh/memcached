//Package selector determine which memcached server to talk to
package selector

import (
    "hash/crc32"
    "errors"
)

func SelectServer(servers []string, key string) (index uint32, err error) {
    defer func(){
        if r := recover(); r != nil {
            err = errors.New(r.(string))
        }
    }()
    
    if(len(servers) == 0) {
        panic("servers must not empty.")
    }

    if len(key) == 0 {
        panic("key must not empty.")
    }

    hashCode := crc32.ChecksumIEEE([]byte(key))
    index = hashCode % uint32(len(servers))

    return
}