package kafka

import (
	"cloudkarafka-mgmt/store"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func metricMessage(msg *kafka.Message) {
	keys, err := parseKey(string(msg.Key))
	if err != nil {
		fmt.Println("[ERROR]", err)
		return
	}
	value, err := parseBody(msg.Value)
	if err != nil {
		fmt.Println("[ERROR]", string(msg.Value))
		fmt.Println(err)
		return
	}
	ts := msg.Timestamp.UTC().Unix()
	switch keys["domain"] {
	case "kafka.log":
		storeLogOffset(keys, value, ts)
	case "kafka.server":
		storeKafkaServer(keys, value, ts)
	}
	storeKafkaStats(keys, value, ts)
}

func storeKafkaServer(keys map[string]string, value map[string]interface{}, ts int64) {
	brokerId, _ := value["BrokerId"].(float64)
	broker := fmt.Sprintf("%v", brokerId)
	switch keys["type"] {
	case "app-info":
		kafkaVersion(broker, value["Version"])
	case "socket-server-metrics":
		socketServerMetrics(broker, keys, value, ts)
	case "BrokerTopicMetrics":
		brokerTopicMetrics(broker, keys, value, ts)
	case "ReplicaManager":
		replicaManager(broker, keys, value, ts)
	}
}

type Metric []string

func (v Metric) Matches(keys map[string]string) bool {
	return keys["domain"] == v[0] &&
		(v[1] == "*" || keys["type"] == v[1]) &&
		(v[2] == "*" || keys["name"] == v[2]) &&
		(v[3] == "*" || keys["request"] == v[3])
}
func (v Metric) String() string {
	return strings.Join(v, ".")
}

var statsMetrics = []Metric{
	Metric{"kafka.network", "RequestMetrics", "TotalTimeMs", "Produce", "Mean"},
	Metric{"kafka.network", "RequestMetrics", "TotalTimeMs", "Fetch", "Mean"},
	Metric{"kafka.network", "RequestChannel", "RequestQueueSize", "*", "Mean"},
	Metric{"kafka.server", "ReplicaManager", "UnderReplicatedPartitions", "*", "OneMinuteRate"},
}

func storeKafkaStats(keys map[string]string, value map[string]interface{}, ts int64) {
	brokerId, _ := value["BrokerId"].(float64)
	id := strconv.Itoa(int(brokerId))
	for _, m := range statsMetrics {
		if m.Matches(keys) {
			for k, v := range value {
				if k == "BrokerId" {
					continue
				}
				keys["measure"] = k
				/*data := store.Data{
					Tags:      keys,
					Value:     int(v.(float64)),
					Timestamp: ts,
				}*/
				store.Put("broker", int(v.(float64)), ts, keys["name"], id, keys["request"])
			}
			return
		}
	}
}

func storeLogOffset(keys map[string]string, value map[string]interface{}, ts int64) {
	//brokerId, _ := value["BrokerId"].(float64)
	//id := strconv.Itoa(int(brokerId))
	v, ok := value["Value"].(float64)
	if !ok {
		return
	}
	store.Put("topic", int(v), ts, keys["name"], keys["topic"], keys["partition"])
}

func kafkaVersion(broker string, version interface{}) {
	if val, ok := version.(string); ok {
		store.Put("broker", 0, 0, "KafkaVersion", broker, val)
	}
}

func socketServerMetrics(broker string, keys map[string]string, value map[string]interface{}, ts int64) {
	if keys["listener"] == "" {
		return
	}
	val, _ := value["connection-count"].(float64)
	store.Put("broker", int(val), ts, "ConnectionCount", broker, keys["listener"])
}

func brokerTopicMetrics(broker string, keys map[string]string, value map[string]interface{}, ts int64) {
	topic := keys["topic"]
	val, _ := value["OneMinuteRate"].(float64)
	brokerId, _ := value["BrokerId"].(float64)
	id := strconv.Itoa(int(brokerId))
	if topic == "" {
		store.Put("broker", int(val), ts, keys["name"], id)
	} else {
		store.Put("topic", int(val), ts, keys["name"], topic)
	}
}

func replicaManager(broker string, keys map[string]string, value map[string]interface{}, ts int64) {
	if val, ok := value["OneMinuteRate"]; ok {
		store.Put("broker", int(val.(float64)), ts, keys["name"], broker)
	} else if val, ok := value["Value"]; ok {
		store.Put("broker", int(val.(float64)), ts, keys["name"], broker)
	}
}

func parseKey(key string) (map[string]string, error) {
	keys := make(map[string]string)
	mbeanName := strings.Split(key, ",")
	if len(mbeanName) == 0 {
		return keys, fmt.Errorf("Unknown format for message key: %s", key)
	}
	for _, m := range mbeanName {
		kv := strings.Split(m, "=")
		if len(kv) != 2 {
			fmt.Println(m)
			continue
		}
		keys[kv[0]] = kv[1]
	}
	return keys, nil
}

func parseBody(bytes []byte) (map[string]interface{}, error) {
	value := make(map[string]interface{})
	err := json.Unmarshal(bytes, &value)
	if err != nil {
		return value, err
	}
	return value, nil
}
