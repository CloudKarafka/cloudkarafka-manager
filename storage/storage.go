package storage

import (
	"fmt"
	"sync"
)

var (
	lock    = sync.Mutex{}
	storage = make(map[string]interface{})
)

func Add(key string, value interface{}) {
	lock.Lock()
	storage[key] = value
	fmt.Println(key, value)
	lock.Unlock()
}

func Get(key string) interface{} {
	return storage[key]
}
