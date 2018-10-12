package zookeeper

import (
	"fmt"
	"strconv"

	"github.com/samuel/go-zookeeper/zk"
)

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

type partitionList [][]int

func (pl partitionList) toReassignments(topic string) []reassignment {
	res := make([]reassignment, len(pl))
	for i, r := range pl {
		res[i] = reassignment{
			topic,
			i,
			r,
		}
	}
	return res
}
func (pl partitionList) toTopicMap() map[string][]int {
	res := make(map[string][]int)
	for i, r := range pl {
		res[strconv.Itoa(i)] = r
	}
	return res
}

func Partition(t, num string) (P, error) {
	var p P
	err := get(fmt.Sprintf("/brokers/topics/%s/partitions/%s/state", t, num), &p)
	p.Topic = t
	p.Number = num
	return p, err
}

func updatePartitions(brokers []int, pl partitionList, replicationFactor, partitionCount int) partitionList {
	spread := replicationFactor != len(pl[0])
	currentPartitionCount := len(pl)
	if partitionCount > currentPartitionCount {
		newPl := make(partitionList, partitionCount-currentPartitionCount)
		if !spread {
			newPl = spreadPartitions(newPl, brokers, replicationFactor)
		}
		pl = append(pl, newPl...)
	}
	if spread {
		pl = spreadPartitions(pl, brokers, replicationFactor)
	}
	return pl
}

func spreadPartitions(pl partitionList, brokers []int, replicationFactor int) partitionList {
	perms := replicaSpreads(brokers, replicationFactor)
	l := len(perms)
	for i := range pl {
		pl[i] = perms[i%l]
	}
	return pl
}

func reassigningInProgress(topic string) (bool, error) {
	var (
		node = make(map[string]interface{})
		rs   = []reassignment{}
	)
	err := get("/admin/reassign_partitions", &node)
	if err == zk.ErrNoNode {
		return false, nil
	} else if err != nil {
		return false, err
	}
	rs, ok := node["partitions"].([]reassignment)
	if !ok {
		return false, unexpectedFormat
	}
	return len(rs) > 0, nil
}

func replicaSpreads(brokers []int, replicationFactor int) partitionList {
	permuts := permutation(brokers)
	l := len(permuts)
	batchSize := len(permuts) / len(brokers)
	var batches [][][]int
	for batchSize < len(permuts) {
		permuts, batches = permuts[batchSize:], append(batches, permuts[0:batchSize:batchSize])
	}
	batches = append(batches, permuts)
	res := make(partitionList, l)
	index := 0
	for j := 0; j < batchSize; j++ {
		for i := 0; i < len(batches); i++ {
			res[index] = batches[i][j][0:replicationFactor]
			index++
		}

	}
	return res
}

func permutation(xs []int) (permuts [][]int) {
	var rc func([]int, int)
	rc = func(a []int, k int) {
		if k == len(a) {
			permuts = append(permuts, append([]int{}, a...))
		} else {
			for i := k; i < len(xs); i++ {
				a[k], a[i] = a[i], a[k]
				rc(a, k+1)
				a[k], a[i] = a[i], a[k]
			}
		}
	}
	rc(xs, 0)
	return permuts
}
