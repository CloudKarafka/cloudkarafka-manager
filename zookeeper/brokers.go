package zookeeper

import (
	"fmt"
)

type B struct {
	Version   int      `json:"-"`
	JmxPort   int      `json:"jmx_port"`
	Timestamp string   `json:"timestamp"`
	Endpoints []string `json:"endpoints"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Id        string   `json:"id"`
}

func Brokers() ([]string, error) {
	return all("/brokers/ids", func(string) bool { return true })
}

func Broker(id string) (B, error) {
	var b B
	err := get(fmt.Sprintf("/brokers/ids/%s", id), &b)
	b.Id = id
	return b, err
}
