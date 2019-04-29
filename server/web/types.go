package web

import (
	"net/url"

	m "github.com/cloudkarafka/cloudkarafka-manager/metrics"
)

type User struct {
	Name string
	Type string
}

type Topic struct {
	m.Topic
	Config   m.TopicConfig
	BytesIn  int
	BytesOut int
}

type TopicConfig struct {
	Key       string
	ValueType string
	Options   []string
	Default   interface{}
}

type TopicForm struct {
	ConfigOptions []TopicConfig
	Errors        []string
	Values        map[string]string
}

func (me *TopicForm) LoadValues(values url.Values) {
	me.Values = make(map[string]string)
	for k, _ := range values {
		v := values.Get(k)
		if v != "" {
			me.Values[k] = v
		}
	}
}
