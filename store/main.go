package store

import (
	"errors"
	"sync"
)

var (
	lock         sync.RWMutex
	Store        = newStore(0)
	KafkaVersion string

	NotFound = errors.New("Element not found")
)

func Put(data Data, indexOn []string) {
	lock.RLock()
	defer lock.RUnlock()
	Store.Put(data, indexOn)
}

func Intersection(indexNames ...string) store {
	lock.RLock()
	defer lock.RUnlock()
	return Store.Intersection(indexNames...)
}

func SelectWithIndex(indexName string) store {
	lock.RLock()
	defer lock.RUnlock()
	return Store.SelectWithIndex(indexName)
}

func IndexedNames(name string) []string {
	lock.RLock()
	defer lock.RUnlock()
	return Store.IndexedNames(name)
}
