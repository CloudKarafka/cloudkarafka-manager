package web

import (
	"encoding/json"
	"net/url"

	"github.com/cloudkarafka/cloudkarafka-manager/store"
)

type User struct {
	Name string
	Type string
}

type Topic struct {
	store.Topic
	Config   TopicConfig
	BytesIn  int
	BytesOut int
}

type TopicConfig struct {
	Version int                    `json:"version"`
	Data    map[string]interface{} `json:"config"`
}

func (t TopicConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Data)
}

type TopicSetting struct {
	Key       string
	ValueType string
	Options   []interface{}
	Default   interface{}
}

type TopicForm struct {
	ConfigOptions []TopicSetting
	Errors        []string
	Values        map[string]interface{}
}

func (me *TopicForm) LoadValues(values url.Values) {
	me.Values = make(map[string]interface{})
	for k, _ := range values {
		v := values.Get(k)
		if v != "" {
			me.Values[k] = v
		}
	}
}
