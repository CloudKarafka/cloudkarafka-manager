package store

import (
	"errors"
	"fmt"
	"time"
)

var (
	b            = newBackend()
	KafkaVersion = make(map[string]string)

	NotFound = errors.New("Element not found")
)

func Put(data Data, indexOn []string) {
	b.Put(data, indexOn)
}

func Intersection(indexNames ...string) Store {
	return b.Intersection(indexNames...)
}

func SelectWithIndex(indexName string) Store {
	return b.SelectWithIndex(indexName)
}

func IndexedNames(name string) []string {
	return b.IndexedNames(name)
}

func init() {
	go func() {
		for {
			dur := b.GC()
			fmt.Printf("gc-finished duration=%v\n", dur)
			s, ss, is, it := b.Stats()
			fmt.Printf("backend stats size=%v segments=%v index-size=%v index-types=%v\n",
				s, ss, is, it)
			time.Sleep(30 * time.Second)
		}
	}()
}
