package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

func CreateTopic(name string, partitions, replicationFactor int) error {
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
	hash := make(map[string][]int)
	nrOfBrokers := len(ids)
	perms := permutations(ids)
	for i := 0; i < partitions; i++ {
		hash[strconv.Itoa(i)] = perms[i%nrOfBrokers]
	}
	topic["partitions"] = hash
	j, _ := json.Marshal(topic)
	_, err := conn.Create("/brokers/topics/"+name, j, 0, zk.WorldACL(zk.PermAll))
	return err
}

func DeleteTopic(name string) error {
	_, err := conn.Create("/admin/delete_topics/"+name, nil, 0, zk.WorldACL(zk.PermAll))
	return err
}

func permutations(arr []int) [][]int {
	var helper func([]int, int)
	res := [][]int{}

	helper = func(arr []int, n int) {
		if n == 1 {
			tmp := make([]int, len(arr))
			copy(tmp, arr)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++ {
				helper(arr, n-1)
				if n%2 == 1 {
					tmp := arr[i]
					arr[i] = arr[n-1]
					arr[n-1] = tmp
				} else {
					tmp := arr[0]
					arr[0] = arr[n-1]
					arr[n-1] = tmp
				}
			}
		}
	}
	helper(arr, len(arr))
	return res
}
