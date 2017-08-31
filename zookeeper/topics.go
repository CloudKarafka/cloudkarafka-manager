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
		partitions[i] = P{
			Number:   p,
			Replicas: parts,
		}
		i++
	}
	ids, _ := Brokers()
	topic["partitions"] = genPartitions(len(parts), partitionCount, replicationFactor, ids, partitions)
	raw, _ = json.Marshal(topic)
	_, err := conn.Set(path, raw, stat.Version)
	if cfg != nil {
		createOrSetConfig(name, cfg)
	}
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
	partitions := genPartitions(0, partitionCount, replicationFactor, ids, make([]P, partitionCount))
	topic["partitions"] = partitions
	return topic
}

func genPartitions(i, p, r int, ids []string, partitions []P) map[string][]int {
	for ; i < p; i++ {
		replicas := leastPartitions(r, ids, partitions)
		partitions[i] = P{
			Number:   strconv.Itoa(i),
			Replicas: replicas,
		}
		fmt.Println(partitions)
	}
	parts := make(map[string][]int)
	for _, part := range partitions {
		parts[part.Number] = part.Replicas
	}
	return parts
}

func leastPartitions(r int, ids []string, partitions []P) []int {
	lc := make(map[string]leaderCount)
	for _, l := range ids {
		b, _ := strconv.Atoi(l)
		lc[l] = leaderCount{Broker: b, Leader: 0, Follower: 0}
	}
	for _, p := range partitions {
		for i, b := range p.Replicas {
			l := strconv.Itoa(b)
			entry := lc[l]
			if i == 0 {
				entry.Leader += 1
			} else {
				entry.Follower += 1
			}
			lc[l] = entry
		}
	}

	count := make([]leaderCount, len(lc))
	i := 0
	for _, k := range lc {
		count[i] = k
		i++
	}

	replicas := make([]int, r)
	sort.Sort(byLeader{count})
	replicas[0] = count[0].Broker
	count = count[1:]
	sort.Sort(byFollower{count})
	for i = 0; i < (r - 1); i++ {
		replicas[i+1] = count[i].Broker
	}
	return replicas
}
