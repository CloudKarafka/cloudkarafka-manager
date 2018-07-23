package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

var (
	onlyIncreasePartitionCount = errors.New("ERROR: partition count can only increase.")
	invalidReplicationFactor   = errors.New("ERROR: replication factor can not be larger than number of online brokers.")
	topicAlreadyExists         = errors.New("ERROR: topic already exists")
	topicDoesNotExist          = errors.New("ERROR: topic doesn't exist")
	unexpectedFormat           = errors.New("ERROR: unexpected format encountered in ZooKeeper node")
)

type topic struct {
	Version    int              `json:"version"`
	Partitions map[string][]int `json:"partitions"`
}

type T struct {
	Name       string                 `json:"name,omitempty"`
	Config     map[string]interface{} `json:"config"`
	Partitions map[string][]int       `json:"partitions"`
}

type P struct {
	Number   string `json:"number"`
	Topic    string `json:"topic"`
	Leader   int    `json:"leader"`
	Replicas []int  `json:"replicas"`
	Isr      []int  `json:"isr"`
}

type reassignment struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

func (p P) String() string {
	return fmt.Sprintf("Number=%s Topic=%s Replicas=%v", p.Number, p.Topic, p.Replicas)
}

func (p P) ReplicasInclude(id int) bool {
	included := false
	for _, r := range p.Replicas {
		included = included || r == id
	}
	return included
}

func Topics(p Permissions) ([]string, error) {
	return all("/brokers/topics", p.TopicRead)
}

func Topic(name string) (T, error) {
	var t T
	t.Name = name
	err := get("/brokers/topics/"+name, &t)
	if err != nil {
		return t, err
	}
	cfg, err := Config(name)
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

func LeaderFor(t, p string) (B, error) {
	partition, err := Partition(t, p)
	if err != nil {
		return B{}, err
	}
	return Broker(strconv.Itoa(partition.Leader))
}

func Config(name string) ([]byte, error) {
	path := "/config/topics/" + name
	d, _, err := conn.Get(path)
	return d, err
}

func Partition(t, num string) (P, error) {
	var p P
	err := get(fmt.Sprintf("/brokers/topics/%s/partitions/%s/state", t, num), &p)
	p.Topic = t
	p.Number = num
	return p, err
}

func UpdateTopic(name string, partitionCount, replicationFactor int, cfg map[string]interface{}) error {
	path := topicPath(name)
	if exists, _, _ := conn.Exists(path); !exists {
		return topicDoesNotExist
	}
	raw, _, _ := conn.Get(path)
	var t topic
	if err := json.Unmarshal(raw, &t); err != nil {
		return err
	}
	if partitionCount < len(t.Partitions) {
		return onlyIncreasePartitionCount
	}
	t.Partitions = spreadReplicas(partitionCount, replicationFactor, t.Partitions)
	reassignPartitions(name, t.Partitions)
	fmt.Printf("[INFO] partition_count=%v replication_factor=%v\n", partitionCount, replicationFactor)
	err := set(path, t)
	if err != nil {
		return err
	}
	if cfg != nil {
		err = createOrSetConfig(name, cfg)
	}
	return err
}

func SpreadPartitionEvenly(name string) error {
	t, err := Topic(name)
	if err != nil {
		return err
	}
	brokers, err := Brokers()
	if err != nil {
		return err
	}
	ids := make([]int, len(brokers))
	for i, b := range brokers {
		ids[i], _ = strconv.Atoi(b)
	}
	partitions := make(map[string][]int)
	r := len(t.Partitions["0"])
	var id int
	for p, _ := range t.Partitions {
		partitions[p] = ids[:r]
		id, ids = ids[0], ids[1:]
		ids = append(ids, id)
	}
	return reassignPartitions(name, partitions)
}

func ReassigningPartitions(topic string) ([]reassignment, error) {
	var (
		node = make(map[string]interface{})
		rs   = []reassignment{}
	)
	err := get("/admin/reassign_partitions", &node)
	if err == zk.ErrNoNode {
		return rs, nil
	} else if err != nil {
		return rs, err
	}
	rs, ok := node["partitions"].([]reassignment)
	if !ok {
		return rs, unexpectedFormat
	}
	return rs, nil
}

func reassignPartitions(topic string, partitions map[string][]int) error {
	rs, err := ReassigningPartitions(topic)
	if err != nil {
		return err
	}
	if len(rs) > 0 {
		return fmt.Errorf("Reassignment in progress, please wait until it has finished.")
	}
	var reasignments []reassignment
	for part, replicas := range partitions {
		p, _ := strconv.Atoi(part)
		reasignments = append(reasignments, reassignment{
			Topic:     topic,
			Partition: p,
			Replicas:  replicas,
		})
	}
	return createPersistent("/admin/reassign_partitions", map[string]interface{}{
		"version":    1,
		"partitions": reasignments,
	})
}

func CreateTopic(name string, partitionCount, replicationFactor int, cfg map[string]interface{}) error {
	ids, _ := Brokers()
	if replicationFactor > len(ids) {
		return invalidReplicationFactor
	}
	topic := newTopic(name, partitionCount, replicationFactor)
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

func newTopic(name string, partitionCount, replicationFactor int) topic {
	parts := spreadReplicas(partitionCount, replicationFactor, make(map[string][]int))
	return topic{Version: 1, Partitions: parts}
}

func spreadReplicas(partitionCount, replicationFactor int, partitions map[string][]int) map[string][]int {
	brokers, _ := Brokers()
	ids := make([]int, len(brokers))
	for i, b := range brokers {
		ids[i], _ = strconv.Atoi(b)
	}
	sort.Ints(ids)
	for i := 0; i < partitionCount; i++ {
		replicas := partitions[strconv.Itoa(i)]
		if len(replicas) < replicationFactor {
			oReplicas := make(map[int]struct{})
			for _, r := range replicas {
				if _, ok := oReplicas[r]; !ok {
					oReplicas[r] = struct{}{}
				}
			}
			for _, id := range ids {
				if len(replicas) == replicationFactor {
					break
				}
				if _, ok := oReplicas[id]; !ok {
					replicas = append(replicas, id)
				}
			}
			partitions[strconv.Itoa(i)] = replicas
		}
		ids = append(ids[1:], ids[0])
	}
	return partitions
}
