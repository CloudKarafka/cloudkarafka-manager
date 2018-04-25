package store

import (
	//"cloudkarafka-mgmt/zookeeper"
	"sort"
)

type store []Data

func (me store) Each(fn func(Data)) {
	for _, d := range me {
		fn(d)
	}
}

func (me store) Any(fn func(Data) bool) bool {
	selected := me.Select(fn)
	return selected.Len() > 0
}

func (me store) Last() Data {
	len := me.Len()
	if len == 0 {
		return Data{}
	}
	return me[me.Len()-1]
}

func (me store) Select(fn func(Data) bool) store {
	var selected store
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
		return d.Tags["metric"]
	})
}

func (me store) GroupByTopic() map[string]store {
	return me.GroupBy(func(d Data) string {
		return d.Tags["topic"]
	})
}

func (me store) GroupByPartition() map[string]store {
	return me.GroupBy(func(d Data) string {
		return d.Tags["partition"]
	})
}

func (me store) GroupBy(fn func(Data) string) map[string]store {
	grouped := make(map[string]store)
	for _, d := range me {
		name := fn(d)
		if _, ok := grouped[name]; !ok {
			grouped[name] = store{}
		}
		g := grouped[name]
		g = append(g, d)
		grouped[name] = g
	}
	return grouped
}

func (me store) StringMap(fn func(Data) string) []string {
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
