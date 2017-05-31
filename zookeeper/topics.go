package zookeeper

import (
	"fmt"
)

func Topics() ([]string, error) {
	topics, _, err := conn.Children("/brokers/topics")
	if err != nil {
		connect("localhost:2181")
	}
	return topics, nil
}

func Topic(name string) ([]byte, error) {
	topic, _, err := conn.Get("/brokers/topics/" + name)
	if err != nil {
		connect("localhost:2181")
	}
	return topic, err
}

func Partition(topic, partition string) ([]byte, error) {
	path := fmt.Sprintf("/brokers/topics/%s/partitions/%s/state", topic, partition)
	state, _, err := conn.Get(path)
	if err != nil {
		connect("localhost:2181")
	}
	return state, err
}
