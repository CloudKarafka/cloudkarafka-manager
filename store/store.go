package store

import (
	//"cloudkarafka-mgmt/zookeeper"
	"fmt"

	"sort"
	"sync"
	"time"
)

type store struct {
	sync.RWMutex
	Stored     []Data
	indexes    map[string][]int
	indexTypes map[string][]string
}

func newStore(size int) store {
	return store{
		Stored:     make([]Data, size),
		indexes:    make(map[string][]int),
		indexTypes: make(map[string][]string),
	}
}

func (me store) Intersection(indexNames ...string) store {
	me.RLock()
	defer me.RUnlock()
	var ints []int
	for _, name := range indexNames {
		ints = append(ints, me.indexes[name]...)
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
	return me.subset(intersection)
}

func (me store) IndexTypes() map[string][]string {
	me.RLock()
	defer me.RUnlock()
	return me.indexTypes
}

func (me store) Index(i string) []int {
	me.RLock()
	defer me.RUnlock()
	return me.indexes[i]
}

func (me store) IndexedNames(indexName string) []string {
	me.RLock()
	defer me.RUnlock()
	return me.indexTypes[indexName]
}

func (me store) SelectWithIndex(indexName string) store {
	me.RLock()
	defer me.RUnlock()
	return me.subset(me.indexes[indexName])
}

func (me store) subset(ints []int) store {
	me.RLock()
	defer me.RUnlock()
	selected := newStore(len(ints))
	for i, id := range ints {
		if len(me.Stored) <= id {
			fmt.Printf("[ERROR] id-larger-than-stored id=%v len=%v\n",
				id, len(me.Stored))
			continue
		}
		selected.Stored[i] = me.Stored[id]
	}
	return selected
}

func (me store) Select(fn func(Data) bool) store {
	var selected store
	me.RLock()
	defer me.RUnlock()
	for _, d := range me.Stored {
		if fn(d) {
			selected.Stored = append(selected.Stored, d)
		}
	}
	return selected
}

func (me store) Find(fn func(Data) bool) (Data, error) {
	data := me.Select(fn)
	if len(data.Stored) != 1 {
		return Data{}, NotFound
	}
	return data.Stored[0], nil
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
	me.RLock()
	defer me.RUnlock()
	grouped := make(map[string]store)
	for _, d := range me.Stored {
		name := fn(d)
		if _, ok := grouped[name]; !ok {
			grouped[name] = store{}
		}
		g := grouped[name]
		g.Stored = append(g.Stored, d)
		grouped[name] = g
	}
	return grouped
}

func (me store) StringMap(fn func(Data) string) []string {
	me.RLock()
	defer me.RUnlock()
	strings := make([]string, len(me.Stored))
	for i, d := range me.Stored {
		strings[i] = fn(d)
	}
	return strings
}

func (me store) Sort() store {
	sort.Sort(me)
	return me
}

func (me store) Len() int {
	me.RLock()
	defer me.RUnlock()
	return len(me.Stored)
}

func (me store) Less(i, j int) bool {
	iElem, jElem := me.Stored[i], me.Stored[j]
	return iElem.Timestamp < jElem.Timestamp
}

func (me store) Swap(i, j int) {
	iElem, jElem := me.Stored[i], me.Stored[j]
	me.Stored[j] = iElem
	me.Stored[i] = jElem
}

func (me *store) Put(data Data, indexOn []string) {
	me.Lock()
	defer me.Unlock()
	me.put(data, indexOn)
}

func (me *store) put(data Data, indexOn []string) {
	if data.Timestamp == 0 {
		data.Timestamp = time.Now().UTC().Unix()
	}
	for _, n := range indexOn {
		index, ok := data.Id[n]
		if !ok {
			continue
		}
		if _, ok = me.indexes[index]; !ok {
			me.indexTypes[n] = append(me.indexTypes[n], index)
		}
		me.indexes[index] = append(me.indexes[index], len(me.Stored))
	}
	me.Stored = append(me.Stored, data)
}

func (me *store) Copy(data map[string]puttable) {
	me.Lock()
	defer me.Unlock()
	me.Stored = make([]Data, 0)
	me.indexes = make(map[string][]int)
	me.indexTypes = make(map[string][]string)
	for _, p := range data {
		me.put(p.D, p.IndexNames)
	}
}
