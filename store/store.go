package store

import (
	//"cloudkarafka-mgmt/zookeeper"

	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
)

type storageType int

const (
	JMX storageType = 1 + iota
	ConsumerOffset
)

func (me storageType) String() string {
	return strconv.Itoa(int(me))
}

var (
	Store        store
	l            sync.RWMutex
	KafkaVersion string

	NotFound     = errors.New("Element not found")
	indexes      = make(map[string][]int)
	indexedNames = make(map[string][]string)
)

type store []Data
type set map[string]bool

type Data struct {
	Value     int
	Timestamp int64
	Id        map[string]string
}

func IndexedNames(name string) []string {
	return indexedNames[name]
}

func Intersection(indexNames ...string) store {
	var ints []int
	for _, name := range indexNames {
		ints = append(ints, indexes[name]...)
	}
	sort.Ints(ints)
	var intersection []int
	var prev int
	prev, ints = ints[0], ints[1:]
	for _, val := range ints {
		if prev == val {
			len := len(intersection)
			if len == 0 || intersection[len-1] != val {
				intersection = append(intersection, val)
			}
		}
		prev = val
	}
	return selectWithIndex(intersection)
}

func SelectWithIndex(indexName string) store {
	positions := indexes[indexName]
	return selectWithIndex(positions)
}

/*func Intersection(indices []string) store {
	var ids []int
	for _, index := range indices {
		i := len(ids)
		ids = append(ids[:i], append(SelectWithIndex(id), ids[i:]...)...)
	}
	intSlice := sort.IntSlice(ids)
	intSlice.Sort()
	prev := intSlice[0]
	ints := make(map[int]struct{})
	var ids []int
	for _, id := range intSlice[1:] {
		if id == prev && _, exists := ints[id]; !exists {
			ints[id] = struct{}{}
			ids = append(i, id)
		}
		prev = id
	}
	return selectWithIndex(ids)
}*/

func selectWithIndex(ints []int) store {
	var selected []Data
	for _, i := range ints {
		selected = append(selected, Store[i])
	}
	return selected
}

func (me store) Select(fn func(Data) bool) store {
	var selected store
	l.RLock()
	defer l.RUnlock()
	for _, d := range me {
		if fn(d) {
			selected = append(selected, d)
		}
	}
	return selected
}

func (me store) Find(fn func(Data) bool) (Data, error) {
	data := me.Select(fn)
	if len(data) != 1 {
		return Data{}, NotFound
	}
	return data[0], nil
}

func (me store) GroupByMetric() map[string]store {
	return me.GroupBy(func(d Data) string {
		return d.Id["metric"]
	})
}

func (me store) GroupByTopic() map[string]store {
	return me.GroupBy(func(d Data) string {
		return d.Id["topic"]
	})
}

func (me store) GroupByPartition() map[string]store {
	return me.GroupBy(func(d Data) string {
		return d.Id["partition"]
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

func (me store) Sort() store {
	sort.Sort(me)
	return me
}

func (me store) Len() int {
	return len(me)
}

func (me store) Less(i, j int) bool {
	iElem, jElem := me[i], me[j]
	return iElem.Timestamp < jElem.Timestamp
}

func (me store) Swap(i, j int) {
	iElem, jElem := me[i], me[j]
	me[j] = iElem
	me[i] = jElem
}

func Put(data Data, indexOn []string) {
	l.Lock()
	defer l.Unlock()
	if data.Timestamp == 0 {
		data.Timestamp = time.Now().UTC().Unix()
	}
	for _, n := range indexOn {
		index, ok := data.Id[n]
		if !ok {
			continue
		}
		if _, ok = indexes[index]; !ok {
			indexes[index] = make([]int, 0)
			indexedNames[n] = append(indexedNames[n], index)
		}
		indexes[index] = append(indexes[index], len(Store))
	}
	Store = append(Store, data)
	/*Data{
		Value:     val,
		Timestamp: time.Now().UTC().Unix(),
		Properties:        id,
	})*/
}
