package zookeeper

/*

import (
	"github.com/samuel/go-zookeeper/zk"

	"fmt"
	"strconv"
)

type Message struct {
	metric  string
	cluster string

	Type      string `json:"type"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Group     string `json:"group"`
	Timestamp int64  `json:"timestamp"`
	Offset    int64  `json:"offset"`
}

func consumerOffset(cluster string) error {
	conn := conns[cluster]
	_, _, newGroups, err := conn.ChildrenW("/consumers")
	if err != nil {
		return err
	}

	go func(conn *zk.Conn, newGroups <-chan zk.Event) {
		for e := range newGroups {
			fmt.Println(e)
			newGroup(cluster)
		}
	}(conn, newGroups)
	return nil
}

func newGroup(cluster string) {
	conn := conns[cluster]
	groups, _, err := conn.Children("/consumers")
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, g := range groups {
		topics(cluster, g)
	}
}

func topics(cluster, group string) {
	conn := conns[cluster]
	path := fmt.Sprintf("/consumers/%s/offsets", group)
	fmt.Println(path)
	// /consumers/[groupId]/offsets/[topic]/[partitionId] -> long (offset)
	topics, _, err := conn.Children(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, t := range topics {
		partitions(cluster, group, t)
	}
}

func partitions(cluster, group, topic string) {
	conn := conns[cluster]
	path := fmt.Sprintf("/consumers/%s/offsets/%s", group, topic)
	// /consumers/[groupId]/offsets/[topic]/[partitionId] -> long (offset)
	partitions, _, err := conn.Children(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, p := range partitions {
		path = fmt.Sprintf("/consumers/%s/offsets/%s/%s", group, topic, p)
		offset, _, err := conn.Get(path)
		if err != nil {
			fmt.Println(err)
			return
		}
		off, _ := strconv.Atoi(string(offset))
		part, _ := strconv.Atoi(p)
		msg := Message{
			metric:    "consumer-offsets",
			Type:      "zookeeper",
			cluster:   cluster,
			Group:     group,
			Topic:     topic,
			Partition: int32(part),
			Offset:    int64(off),
		}
		fmt.Println(msg)
		Messages <- msg
	}
}

func Stop() {
	for _, c := range conns {
		c.Close()
	}
}
*/
