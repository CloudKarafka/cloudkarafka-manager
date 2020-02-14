package zookeeper

import (
	"encoding/json"
	"errors"
)

var (
	onlyIncreasePartitionCount = errors.New("ERROR: partition count can only increase.")
	invalidReplicationFactor   = errors.New("ERROR: replication factor can not be larger than number of online brokers.")
	topicAlreadyExists         = errors.New("ERROR: topic already exists")
	topicDoesNotExist          = errors.New("ERROR: topic doesn't exist")
	unexpectedFormat           = errors.New("ERROR: unexpected format encountered in ZooKeeper node")
)

type T struct {
	Name       string                 `json:"name,omitempty"`
	Config     map[string]interface{} `json:"config"`
	Partitions map[string][]int       `json:"partitions"`
}

func Topics(p Permissions) ([]string, error) {
	return all("/brokers/topics", p.DescribeTopic)
}

func Topic(name string) (T, error) {
	return fetchTopicInfo(name)
}

func fetchTopicInfo(name string) (T, error) {
	var t T
	t.Name = name
	err := get("/brokers/topics/"+name, &t)
	if err != nil {
		return t, err
	}
	cfg, err := TopicConfig(name)
	if err != nil {
		return t, nil
	}
	config := make(map[string]interface{})
	json.Unmarshal(cfg, &config)
	if config, ok := config["config"].(map[string]interface{}); ok {
		t.Config = config
	}
	return t, nil
}

func TopicConfig(name string) ([]byte, error) {
	path := "/config/topics/" + name
	d, _, err := conn.Get(path)
	return d, err
}

var topicsListeners = make([]chan []T, 0, 10)

func WatchTopics() chan []T {
	ch := make(chan []T)
	topicsListeners = append(topicsListeners, ch)
	return ch
}

func watchTopics() {
	data, _, events, _ := WatchChildren("/brokers/topics")
	list := make([]T, len(data))
	for i, name := range data {
		topic, err := Topic(name)
		if err != nil {
			continue
		}
		list[i] = topic
	}
	fanoutCH <- list
	_, ok := <-events
	if ok {
		watchTopics()
	}
}
