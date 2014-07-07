#Memcache client for Go

##How to install
```
$ go get github.com/ningjh/memcached
```

##How to use
```js
// import package
import (
    "github.com/ningjh/memcached"
    "github.com/ningjh/memcached/common"
    
    "fmt"
)

// create a memcached client
var conf = &config.Config{
    Servers      : []string{"127.0.0.1:11211", "127.0.0.1:11212"},
    InitConns    : 10,    //connection pool size
    ReadTimeout  : 3000,  //connection read timeout, 3 seconds
    WriteTimeout : 3000,  //connection write timeout, 3 seconds
}
var memcachedClient, err = memcached.NewMemcachedClient4T(conf)
if err != nil {
    //something wrong.
}

// set an item
var element = &common.Element{
    Key     : "abcd",
    Flags   : 1,
    Exptime : 30, //second
    Value   : []byte("memcached client"),
}
err = memcachedClient.Add(element)

// get an item
item := memcachedClient.Get("abcd")
if item != nil {
    key   := item.Key()
    value := item.Value()
    flags := item.Flags()
    cas   := item.Cas()
}

// get items
keys  := []string{"abc", "def", "ghi"}
items := memcachedClient.GetArray(keys)
if items != nil {
    for _, key := range items {
        if item, ok := items[key]; ok {
            key   := item.Key()
            value := item.Value()
            flags := item.Flags()
            cas   := item.Cas()
        }
    }
}
```
[More Examples]
[More Examples]: https://github.com/ningjh/memcached/blob/master/test/memcached_client_test.go "More Examples"
