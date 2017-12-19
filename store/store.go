package store

import (
	//"cloudkarafka-mgmt/zookeeper"

	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type storageType int

const (
	JMX storageType = 1 + iota
	Group
)

func (me storageType) String() string {
	return strconv.Itoa(int(me))
}

var (
	Store store
	l     sync.RWMutex

	NotFound = errors.New("Element not found")
)

type store []Data
type typeIndex map[storageType][]int

type Data struct {
	Value     int
	Timestamp int64
	Id        map[string]string
}

func (me store) Select(fn func(Data) bool) store {
	var reduced store
	l.RLock()
	defer l.RUnlock()
	for _, d := range me {
		if fn(d) {
			reduced = append(reduced, d)
		}
	}
	return reduced
}

func (me store) Find(fn func(Data) bool) (Data, error) {
	data := me.Select(fn)
	if len(data) != 1 {
		return Data{}, NotFound
	}
	return data[0], nil
}

func (me store) GroupByTopic() map[string]store {
	return me.GroupBy(func(d Data) string {
		return d.Id["topic"]
	})
}

func (me store) GroupBy(fn func(Data) string) map[string]store {
	l.RLock()
	defer l.RUnlock()
	group := make(map[string]store)
	for _, d := range me {
		name := fn(d)
		if _, ok := group[name]; !ok {
			group[name] = make(store, 0)
		}
		group[name] = append(group[name], d)
	}
	return group
}

func (me store) StringMap(fn func(Data) string) []string {
	l.RLock()
	defer l.RUnlock()
	strings := make([]string, len(me))
	for i, d := range me {
		strings[i] = fn(d)
	}
	return strings
}

func Put(data map[string]string, val int) {
	l.Lock()
	defer l.Unlock()
	fmt.Println(Store)
	Store = append(Store, Data{
		Value:     val,
		Timestamp: time.Now().UTC().Unix(),
		Id:        data,
	})
}
