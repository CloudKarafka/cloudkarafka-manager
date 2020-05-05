package store

import (
	"encoding/json"
)

type ConsumerGroupClient struct {
	Broker        int    `json:"-"`
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	CurrentOffset int    `json:"offset"`
	ClientId      string `json:"id"`
	ConsumerId    string `json:"consumer_id"`
	Host          string `json:"host"`
	LogEnd        int    `json:"log_end"`
	LogStart      int    `json:"log_start"`
}

func (cgc *ConsumerGroupClient) setMetric(name string, value int) {
	switch name {
	case "LogEndOffset":
		cgc.LogEnd = value
	case "LogStartOffset":
		cgc.LogStart = value
	}
}

type ConsumerGroup struct {
	Name     string                `json:"name"`
	LastSeen int64                 `json:"last_seen"`
	Clients  []ConsumerGroupClient `json:"clients"`
}

func (cg ConsumerGroup) UpdateTopic(topic string, partition int, name string, value int) {
	for i, c := range cg.Clients {
		if c.Topic == topic && c.Partition == partition {
			cg.Clients[i].setMetric(name, value)
		}
	}
}

func (cg ConsumerGroup) Topics() []string {
	temp := make(map[string]interface{})
	for _, c := range cg.Clients {
		temp[c.Topic] = c.Topic
	}
	res := make([]string, len(temp))
	i := 0
	for t, _ := range temp {
		res[i] = t
		i += 1
	}
	return res
}

func (cg ConsumerGroup) UniqueClients() []string {
	temp := make(map[string]interface{})
	for _, c := range cg.Clients {
		temp[c.ConsumerId] = c
	}
	res := make([]string, len(temp))
	i := 0
	for t, _ := range temp {
		res[i] = t
		i += 1
	}
	return res
}

func (cg ConsumerGroup) MarshalJSON() ([]byte, error) {
	res := map[string]interface{}{
		"name":           cg.Name,
		"topics":         cg.Topics(),
		"clients":        cg.Clients,
		"unique_clients": cg.UniqueClients(),
		"online":         len(cg.Clients) > 0,
	}
	return json.Marshal(res)
}

type ConsumerGroups []ConsumerGroup

func (cgs ConsumerGroups) Get(i int) interface{} {
	return cgs[i]
}
func (cgs ConsumerGroups) Size() int {
	return len(cgs)
}
