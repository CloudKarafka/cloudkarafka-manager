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

type partition struct {
	Number   string
	Replicas []int
}

type leaderCount struct {
	Leader string
	Count  int
}

type byLeaderCount []leaderCount

func (a byLeaderCount) Len() int           { return len(a) }
func (a byLeaderCount) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLeaderCount) Less(i, j int) bool { return a[i].Count < a[j].Count }

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

func UpdateTopic(name string, partitionCount, replicationFactor int) error {
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
	partitions := make([]partition, partitionCount)
	i := 0
	for p, r := range parts {
		parts := make([]int, len(r.([]interface{})))
		for i, pr := range r.([]interface{}) {
			parts[i] = int(pr.(float64))
		}
		partitions[i] = partition{
			Number:   p,
			Replicas: parts,
		}
		i++
	}
	ids := Brokers()
	topic["partitions"] = genPartitions(len(parts), partitionCount, replicationFactor, ids, partitions)
	raw, _ = json.Marshal(topic)
	_, err := conn.Set(path, raw, stat.Version)
	return err
}

func CreateTopic(name string, partitionCount, replicationFactor int) error {
	if replicationFactor > len(Brokers()) {
		return invalidReplicationFactor
	}
	topic := topic(name, partitionCount, replicationFactor)
	j, _ := json.Marshal(topic)
	path := topicPath(name)
	if exists, _, _ := conn.Exists(path); exists {
		return topicAlreadyExists
	}
	_, err := conn.Create(path, j, 0, zk.WorldACL(zk.PermAll))
	return err
}

func DeleteTopic(name string) error {
	_, err := conn.Create("/admin/delete_topics/"+name, nil, 0, zk.WorldACL(zk.PermAll))
	return err
}

func topicPath(name string) string {
	return "/brokers/topics/" + name
}

func topic(name string, partitionCount, replicationFactor int) map[string]interface{} {
	topic := make(map[string]interface{})
	topic["version"] = 1
	ids := Brokers()
	partitions := genPartitions(0, partitionCount, replicationFactor, ids, make([]partition, partitionCount))
	topic["partitions"] = partitions
	return topic
}

func genPartitions(i, p, r int, ids []string, partitions []partition) map[string][]int {
	for ; i < p; i++ {
		replicas := leastPartitions(r, ids, partitions)
		partitions[i] = partition{
			Number:   strconv.Itoa(i),
			Replicas: replicas,
		}
	}
	parts := make(map[string][]int)
	for _, part := range partitions {
		parts[part.Number] = part.Replicas
	}
	return parts
}

func leastPartitions(r int, ids []string, partitions []partition) []int {
	count := make(map[string]int)
	for _, l := range ids {
		count[l] = 0
	}
	for _, p := range partitions {
		if len(p.Replicas) == 0 {
			continue
		}
		count[strconv.Itoa(p.Replicas[0])] += 1
	}

	lc := make([]leaderCount, len(count))
	i := 0
	for l, c := range count {
		lc[i] = leaderCount{Leader: l, Count: c}
		i++
	}

	sort.Sort(byLeaderCount(lc))
	replicas := make([]int, r)
	for i := 0; i < r; i++ {
		l, _ := strconv.Atoi(lc[i].Leader)
		replicas[i] = l
	}
	return replicas
}
