#Memcache client for Go

##简介
    这是一个用Go语言实现的Memcached客户端。主要用以下特性：
    
    1. 实现了Memcached的文本协议（二进制协议稍后会实现）。
    2. 使用了连接池，复用TCP连接。
    3. 实现了Consistent Hashing，如Cache服务器宕机，能够使得影响降到最低。

##安装
```
$ go get github.com/ningjh/memcached
```

##使用
```js

package main

// import package
import (
    "github.com/ningjh/memcached"
    "github.com/ningjh/memcached/common"
    "github.com/ningjh/memcached/config"
    
    "fmt"
)

func main() {
    // 创建配置实例
    var conf = config.New()
    
    conf.Servers          = []string{"127.0.0.1:11211", "127.0.0.1:11212"}//配置Cache服务器列表
    conf.ReadTimeout      = 3000 //配置TCP连接读超时，设为3秒（默认不超时）
    conf.WriteTimeout     = 3000 //配置TCP连接写超时，设为3秒（默认不超时）
    conf.InitConns        = 15   //配置连接池最大容量（默认为15）
    conf.NumberOfReplicas = 20   //配置Cache服务器的虚拟节点数量（默认为20）

    // 创建客户端实例
    var client, err = memcached.NewMemcachedClient4T(conf)
    if err != nil {
        return
    }
    
    // 保存数据
    var element = &common.Element{
        Key     : "test",
        Flags   : 1,
        Exptime : 30, //过期时间30秒
        Value   : []byte("memcached client"),
    }
    err = memcachedClient.Add(element)
    
    // 单个key获取数据
    item, err := memcachedClient.Get("test")
    if err == nil {
        key   := item.Key()
        value := item.Value()
        flags := item.Flags()
        cas   := item.Cas()
    }
    
    // 多个key获取数据
    keys  := []string{"abc", "def", "ghi"}
    items, err := memcachedClient.GetArray(keys)
    if err == nil {
        for _, key := range items {
            if item, ok := items[key]; ok {
                key   := item.Key()
                value := item.Value()
                flags := item.Flags()
                cas   := item.Cas()
            }
        }
    }
}
```
[更多用例]
[更多用例]: https://github.com/ningjh/memcached/blob/master/test/memcached_client_test.go "更多用例"

##文档
[文档]
[文档]: http://godoc.org/github.com/ningjh/memcached "文档"
