package zookeeper

import (
	"cloudkarafka-mgmt/storage"
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"fmt"
)

func clusterInfo(conn *zk.Conn) {
	for {
		ids, _, changes, err := conn.ChildrenW("/brokers/ids")
		if err != nil {
			fmt.Println("error")
		}
		for _, id := range ids {
			storage.Add("id", id)
			raw, _, _ := conn.Get(fmt.Sprintf("/brokers/ids/%s", id))
			info := make(map[string]interface{})
			err := json.Unmarshal(raw, &info)
			fmt.Println(err)
			for key, value := range info {
				storage.Add(key, value)
			}
		}
		<-changes
	}
}
