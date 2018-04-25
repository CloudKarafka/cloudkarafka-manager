package store

import (
	"bytes"
	"sort"
)

type Data struct {
	Value     int
	Timestamp int64
	Tags      map[string]string
}

func (me Data) Key() string {
	keys := make([]string, len(me.Tags))
	i := 0
	for k := range me.Tags {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, k := range keys {
		b.WriteString(me.Tags[k])
	}
	b.WriteString(string(me.Timestamp))
	return b.String()
}
