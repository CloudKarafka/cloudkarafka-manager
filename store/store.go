package store

import (
	//"cloudkarafka-mgmt/zookeeper"
	"sort"
)

type Store []Data

func (me Store) Each(fn func(Data)) {
	for _, d := range me {
		fn(d)
	}
}

func (me Store) Any(fn func(Data) bool) bool {
	selected := me.Select(fn)
	return selected.Len() > 0
}

func (me Store) Last() Data {
	len := me.Len()
	if len == 0 {
		return Data{}
	}
	return me[me.Len()-1]
}

func (me Store) Select(fn func(Data) bool) Store {
	var selected Store
	for _, d := range me {
		if fn(d) {
			selected = append(selected, d)
		}
	}
	return selected
}

func (me Store) Find(fn func(Data) bool) (Data, error) {
	data := me.Select(fn)
	if len(data) != 1 {
		return Data{}, NotFound
	}
	return data[0], nil
}

func (me Store) GroupByMetric() map[string]Store {
	return me.GroupBy(func(d Data) string {
		return d.Tags["metric"]
	})
}

func (me Store) GroupByTopic() map[string]Store {
	return me.GroupBy(func(d Data) string {
		return d.Tags["topic"]
	})
}

func (me Store) GroupByPartition() map[string]Store {
	return me.GroupBy(func(d Data) string {
		return d.Tags["partition"]
	})
}

func (me Store) GroupBy(fn func(Data) string) map[string]Store {
	grouped := make(map[string]Store)
	for _, d := range me {
		name := fn(d)
		if _, ok := grouped[name]; !ok {
			grouped[name] = Store{}
		}
		g := grouped[name]
		g = append(g, d)
		grouped[name] = g
	}
	return grouped
}

func (me Store) StringMap(fn func(Data) string) []string {
	strings := make([]string, len(me))
	for i, d := range me {
		strings[i] = fn(d)
	}
	return strings
}

func (me Store) Sort() Store {
	sort.Sort(me)
	return me
}

func (me Store) Len() int {
	return len(me)
}

func (me Store) Less(i, j int) bool {
	iElem, jElem := me[i], me[j]
	return iElem.Timestamp < jElem.Timestamp
}

func (me Store) Swap(i, j int) {
	iElem, jElem := me[i], me[j]
	me[j] = iElem
	me[i] = jElem
}
