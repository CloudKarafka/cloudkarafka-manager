package kafka

import (
	"github.com/84codes/cloudkarafka-mgmt/store"

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
	case "kafka.controller":
		storeKafkaController(keys, value, ts)
	case "kafka.network":
		storeKafkaNetwork(keys, value, ts)
	case "java.lang":
		storeJvmMetrics(keys, value, ts)
	}
}

func getBrokerId(value map[string]interface{}) string {
	brokerId, _ := value["BrokerId"].(float64)
	return fmt.Sprintf("%v", brokerId)
}

func storeKafkaNetwork(keys map[string]string, value map[string]interface{}, ts int64) {
	brokerId, _ := value["BrokerId"].(float64)
	broker := fmt.Sprintf("%v", brokerId)
	switch keys["type"] {
	case "RequestMetrics":
		s := store.BrokerStore()
		switch keys["name"] {
		case "RequestsPerSec":
			if v, ok := value["OneMinuteRate"].(float64); ok {
				s.RequestCount(broker, int(v), keys["request"])
			}
		case "RequestQueueTimeMs":
			fallthrough
		case "LocalTimeMs":
			fallthrough
		case "RemoteTimeMs":
			fallthrough
		case "ResponseQueueTimeMs":
			fallthrough
		case "ResponseSendTimeMs":
			v := store.NewBrokerRequestValue(value)
			store.BrokerStore().RequestTime(broker, v, keys["name"], keys["request"])
		}
	}
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

func storeLogOffset(keys map[string]string, value map[string]interface{}, ts int64) {
	//brokerId, _ := value["BrokerId"].(float64)
	//id := strconv.Itoa(int(brokerId))
	v, ok := value["Value"].(float64)
	if !ok {
		return
	}
	store.Put("topic", int(v), ts, keys["name"], keys["topic"], keys["partition"])
}

func storeJvmMetrics(keys map[string]string, value map[string]interface{}, ts int64) {
	brokerId, _ := value["BrokerId"].(float64)
	broker := fmt.Sprintf("%v", brokerId)
	switch keys["type"] {
	case "OperatingSystem":
		// Ignore for the moment
	case "GarbageCollector":
		metrics := []string{"G1 Young Generation", "G1 Old Generation"}
		for _, metric := range metrics {
			if keys["name"] == metric {
				c := int(value["CollectionCount"].(float64))
				t := int(value["CollectionTime"].(float64))
				store.Put("jvm", c, ts, broker, metric, "CollectionCount")
				store.Put("jvm", t, ts, broker, metric, "CollectionTime")
			}
		}
	case "Memory":
		metrics := []string{"HeapMemoryUsage", "NonHeapMemoryUsage"}
		for _, metric := range metrics {
			for k, v := range value[metric].(map[string]interface{}) {
				store.Put("jvm", int(v.(float64)), ts, broker, metric, k)
			}
		}
	}
}

func storeKafkaController(keys map[string]string, value map[string]interface{}, ts int64) {
	brokerId := getBrokerId(value)
	switch keys["type"] {
	case "KafkaController":
		v := int(value["Value"].(float64))
		store.Put("broker", v, ts, keys["name"], brokerId)
	case "ControllerStats":
		if keys["name"] == "UncleanLeaderElectionsPerSec" {
			v := int(value["OneMinuteRate"].(float64))
			store.Put("broker", v, ts, keys["name"], brokerId)
		}
	}
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
