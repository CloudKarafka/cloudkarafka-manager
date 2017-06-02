package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

var (
	onlyIncreasePartitionCount = errors.New("ERROR: partition count can only increase.")
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

func CreateOrUpdateTopic(name string, partitionCount, replicationFactor int) error {
	topic := make(map[string]interface{})
	topic["version"] = 1
	var ids []int
	for _, b := range Brokers() {
		id, _ := strconv.Atoi(b)
		ids = append(ids, id)
	}
	if replicationFactor > len(ids) {
		msg := fmt.Sprintf("ERROR: replication factor (%v) can not be larger than number of brokers (%v).", replicationFactor, len(ids))
		return errors.New(msg)
	}
	nrOfBrokers := len(ids)
	partitions := make(map[string][]int)
	for i := 0; i < partitionCount; i++ {
		partitions[strconv.Itoa(i)] = ids[:replicationFactor]
		if nrOfBrokers > 1 {
			ids = append(ids[1:], ids[0])
		}
	}
	topic["partitions"] = partitions
	j, _ := json.Marshal(topic)
	path := "/brokers/topics/" + name
	var err error
	if exists, stat, _ := conn.Exists(path); exists {
		currentPartitions, _, _ := conn.Children(path + "/partitions")
		if len(partitions) < len(currentPartitions) {
			err = onlyIncreasePartitionCount
		} else {
			_, err = conn.Set(path, j, stat.Version)
		}
	} else {
		_, err = conn.Create(path, j, 0, zk.WorldACL(zk.PermAll))
	}
	return err
}

func DeleteTopic(name string) error {
	_, err := conn.Create("/admin/delete_topics/"+name, nil, 0, zk.WorldACL(zk.PermAll))
	return err
}
