package zookeeper

import (
	"fmt"
)

func Brokers() []string {
	ids, _, err := conn.Children("/brokers/ids")
	if err != nil {
		fmt.Println(err)
		connect("localhost:2181")
	}
	return ids
}

func Broker(id string) []byte {
	broker, _, _ := conn.Get(fmt.Sprintf("/brokers/ids/%s", id))
	return broker
}
