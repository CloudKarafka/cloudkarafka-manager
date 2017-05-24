package zookeeper

import (
	"encoding/json"
	"fmt"
)

func Brokers() []map[string]interface{} {
	var brokers []map[string]interface{}
	ids, _, err := conn.Children("/brokers/ids")
	if err != nil {
		fmt.Println(err)
		connect("localhost:2181")
		return brokers
	}
	for _, id := range ids {
		raw, _, _ := conn.Get(fmt.Sprintf("/brokers/ids/%s", id))
		info := make(map[string]interface{})
		json.Unmarshal(raw, &info)
		info["id"] = id
		brokers = append(brokers, info)
	}
	return brokers
}
