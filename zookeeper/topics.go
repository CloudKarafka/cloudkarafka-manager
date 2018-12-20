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

func (topic T) partitionList() partitionList {
	pl := make(partitionList, len(topic.Partitions))
	for i := 0; i < len(pl); i++ {
		pl[i] = topic.Partitions[strconv.Itoa(i)]
	}
	return pl
}

func Topics(p Permissions) ([]string, error) {
	return all("/brokers/topics", p.TopicRead)
}

func fetchTopicInfo(name string, withConfig bool) (T, error) {
	var t T
	t.Name = name
	err := get(topicPath(name), &t)
	if err != nil {
		return t, err
	}
	if withConfig {
		cfg, err := TopicConfig(name)
		if err != nil {
			return t, nil
		}
		config := make(map[string]interface{})
		json.Unmarshal(cfg, &config)
		if config, ok := config["config"].(map[string]interface{}); ok {
			t.Config = config
		}
	}
	return t, nil
}
func Topic(name string) (T, error) {
	return fetchTopicInfo(name, false)
}

func CreateTopic(name string, partitionCount, replicationFactor int, cfg map[string]interface{}) error {
	ids, _ := Brokers()
	if replicationFactor > len(ids) {
		return invalidReplicationFactor
	}
	brokers := make([]int, len(ids))
	for i := 0; i < len(brokers); i++ {
		brokers[i] = i
	}
	pl := make(partitionList, partitionCount)
	pl = spreadPartitions(pl, brokers, replicationFactor)
	topic := T{Partitions: pl.toTopicMap()}
	path := topicPath(name)
	err := createPersistent(path, topic)
	if err == zk.ErrNodeExists {
		return fmt.Errorf("Topic %s already exists.", name)
	}
	if err != nil {
		return err
	}
	if cfg != nil {
		err = createOrSetConfig(name, cfg)
	}
	return err
}

func DeleteTopic(name string) error {
	_, err := conn.Create("/admin/delete_topics/"+name, nil, 0, zk.WorldACL(zk.PermAll))
	return err
}

func UpdateTopic(name string, partitionCount, replicationFactor int, cfg map[string]interface{}) error {
	topic, err := fetchTopicInfo(name, false)
	if partitionCount < len(topic.Partitions) {
		return onlyIncreasePartitionCount
	}
	pl := topic.partitionList()
	bs, _ := Brokers()
	brokers := make([]int, len(bs))
	for i := 0; i < len(brokers); i++ {
		brokers[i] = i
	}
	pl = updatePartitions(brokers, pl, replicationFactor, partitionCount)
	data := map[string]interface{}{
		"version":    1,
		"partitions": pl.toReassignments(name),
	}
	err = createPersistent("/admin/reassign_partitions", data)
	if err != nil {
		return err
	}
	fmt.Printf("[INFO] partition_count=%v replication_factor=%v\n", partitionCount, replicationFactor)
	topic.Partitions = pl.toTopicMap()
	err = set(topicPath(name), topic)
	if err != nil {
		return err
	}
	if cfg != nil {
		err = createOrSetConfig(name, cfg)
	}
	return err
}

func TopicConfig(name string) ([]byte, error) {
	path := "/config/topics/" + name
	d, _, err := conn.Get(path)
	return d, err
}

func createOrSetConfig(name string, cfg map[string]interface{}) error {
	path := "/config/topics/" + name
	var err error
	if d, _, _ := conn.Get(path); d != nil {
		data := make(map[string]interface{})
		json.Unmarshal(d, &data)
		for k, v := range cfg {
			data["config"].(map[string]interface{})[k] = v
		}
		err = set(path, data)
	} else {
		err = createPersistent(path, map[string]interface{}{"version": 1, "config": cfg})
	}
	if err != nil {
		return err
	}
	return createSeq("/config/changes/config_change_", map[string]interface{}{
		"version":     2,
		"entity_path": "topics/" + name,
	})
}

func topicPath(name string) string {
	return "/brokers/topics/" + name
}
