package store

import (
	"bytes"
	"sort"
)

type Data struct {
	Value     int
	Timestamp int64
	Id        map[string]string
}

func (me Data) Key() string {
	keys := make([]string, len(me.Id))
	i := 0
	for k := range me.Id {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, k := range keys {
		b.WriteString(me.Id[k])
	}
	b.WriteString(string(me.Timestamp))
	return b.String()
}
