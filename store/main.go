package store

import (
	"errors"
	"sync"
)

var (
	lock         sync.Mutex
	Store        = newStore(0)
	KafkaVersion string

	NotFound = errors.New("Element not found")
)

func Put(data Data, indexOn []string) {
	Store.Put(data, indexOn)
}

func Intersection(indexNames ...string) store {
	return Store.Intersection(indexNames...)
}

func SelectWithIndex(indexName string) store {
	return Store.SelectWithIndex(indexName)
}

func IndexedNames(name string) []string {
	return Store.IndexedNames(name)
}
