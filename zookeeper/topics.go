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
)

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

type leaderCount struct {
	Broker   int
	Leader   int
	Follower int
}

type leaderCounts []leaderCount

func (a leaderCounts) Len() int      { return len(a) }
func (a leaderCounts) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type byLeader struct{ leaderCounts }

func (a byLeader) Less(i, j int) bool {
	ai, aj := a.leaderCounts[i], a.leaderCounts[j]
	return 2*ai.Leader+ai.Follower < 2*aj.Leader+aj.Follower
}

type byFollower struct{ leaderCounts }

func (a byFollower) Less(i, j int) bool {
	ai, aj := a.leaderCounts[i], a.leaderCounts[j]
	return 2*ai.Follower+ai.Leader < 2*aj.Follower+aj.Leader
}

func Topics(p Permissions) ([]string, error) {
	return all("/brokers/topics", p.TopicRead)
}

func Topic(name string) (T, error) {
	var t T
	err := get("/brokers/topics/"+name, &t)
	t.Name = name
	return t, err
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
	raw, stat, _ := conn.Get(path)
	var topic map[string]interface{}
	if err := json.Unmarshal(raw, &topic); err != nil {
		return err
	}
	parts := topic["partitions"].(map[string]interface{})
	if partitionCount < len(parts) {
		return onlyIncreasePartitionCount
	}
	partitions := make([]P, partitionCount)
	i := 0
	for p, r := range parts {
		parts := make([]int, len(r.([]interface{})))
		for i, pr := range r.([]interface{}) {
			parts[i] = int(pr.(float64))
		}
		partition := P{
			Number:   p,
			Replicas: parts,
		}
		partitions[i] = partition
		i++
	}
	ids, _ := Brokers()
	var expandedPartitions map[string][]int
	topic["partitions"], expandedPartitions = spreadReplicas(partitionCount, replicationFactor, ids, partitions)
	ReassignPartitions(name, expandedPartitions)
	raw, _ = json.Marshal(topic)
	fmt.Printf("[INFO] partition_count=%v replication_factor=%v\n", partitionCount, replicationFactor)
	stat, err := conn.Set(path, raw, stat.Version)
	if cfg != nil {
		createOrSetConfig(name, cfg)
	}
	return err
}

func ReassignPartitions(topic string, partitions map[string][]int) error {
	var reasignment []map[string]interface{}
	for part, replicas := range partitions {
		p, _ := strconv.Atoi(part)
		reasignment = append(reasignment, map[string]interface{}{
			"topic":     topic,
			"partition": p,
			"replicas":  replicas,
		})
	}
	node := map[string]interface{}{
		"version":    1,
		"partitions": reasignment,
	}

	data, _ := json.Marshal(node)
	_, err := conn.Create("/admin/reassign_partitions", data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func CreateTopic(name string, partitionCount, replicationFactor int, cfg map[string]interface{}) error {
	ids, _ := Brokers()
	if replicationFactor > len(ids) {
		return invalidReplicationFactor
	}
	topic := newTopic(name, partitionCount, replicationFactor)
	j, _ := json.Marshal(topic)
	path := topicPath(name)
	if exists, _, _ := conn.Exists(path); exists {
		return topicAlreadyExists
	}
	_, err := conn.Create(path, j, 0, zk.WorldACL(zk.PermAll))
	if cfg != nil {
		createOrSetConfig(name, cfg)
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
	if d, stat, _ := conn.Get(path); d != nil {
		data := make(map[string]interface{})
		json.Unmarshal(d, &data)
		for k, v := range cfg {
			data["config"].(map[string]interface{})[k] = v
		}
		raw, _ := json.Marshal(data)
		_, err = conn.Set(path, raw, stat.Version)
	} else {
		node := map[string]interface{}{"version": 1, "config": cfg}
		raw, _ := json.Marshal(node)
		_, err = conn.Create(path, raw, 0, zk.WorldACL(zk.PermAll))
	}
	return err
}

func topicPath(name string) string {
	return "/brokers/topics/" + name
}

func newTopic(name string, partitionCount, replicationFactor int) map[string]interface{} {
	topic := make(map[string]interface{})
	topic["version"] = 1
	ids, _ := Brokers()
	partitions, _ := spreadReplicas(partitionCount, replicationFactor, ids, make([]P, partitionCount))
	topic["partitions"] = partitions
	return topic
}

func spreadReplicas(partitionCount, replicationFactor int, ids []string, partitions []P) (map[string][]int, map[string][]int) {
	var expand []P
	for i := 0; i < partitionCount; i++ {
		lc := replicaSpread(ids, partitions)
		partition := partitions[i]
		if partition.Number == "" {
			partition.Number = strconv.Itoa(i)
		}
		if len(partition.Replicas) == 0 {
			partitions[i] = assignReplicas(replicationFactor, partition, lc)
		} else {
			expand = append(expand, assignReplicas(replicationFactor, partition, lc))
		}
	}
	parts := make(map[string][]int)
	for _, part := range partitions {
		parts[part.Number] = part.Replicas
	}
	expanded := make(map[string][]int)
	for _, part := range expand {
		expanded[part.Number] = part.Replicas
	}
	fmt.Println(expanded)
	return parts, expanded
}

func assignReplicas(replicationFactor int, partition P, lc []leaderCount) P {
	for len(partition.Replicas) < replicationFactor {
		if len(partition.Replicas) == 0 {
			sort.Sort(byLeader{lc})
			partition.Replicas = append(partition.Replicas, lc[0].Broker)
		} else {
			sort.Sort(byFollower{lc})
			for _, b := range lc {
				if partition.ReplicasInclude(b.Broker) {
					continue
				}
				partition.Replicas = append(partition.Replicas, b.Broker)
				break
			}
		}
	}
	return partition
}

func replicaSpread(ids []string, partitions []P) []leaderCount {
	lc := make([]leaderCount, len(ids))
	for i, l := range ids {
		b, _ := strconv.Atoi(l)
		lc[i] = leaderCount{Leader: 0, Follower: 0, Broker: b}
	}
	for _, p := range partitions {
		for i, id := range p.Replicas {
			entry := lc[id]
			if i == 0 {
				entry.Leader += 1
			} else {
				entry.Follower += 1
			}
			lc[id] = entry
		}
	}
	return lc
}
