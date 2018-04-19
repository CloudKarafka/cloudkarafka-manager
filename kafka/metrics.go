package kafka

import (
	"cloudkarafka-mgmt/store"

	"github.com/Shopify/sarama"

	"encoding/json"
	"fmt"
	"strings"
)

func metricMessage(msg *sarama.ConsumerMessage) {
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

	switch keys["domain"] {
	case "kafka.log":
		storeLogOffset(keys, value)
	case "kafka.server":
		storeKafkaServer(keys, value)
	}
}

func storeKafkaServer(keys map[string]string, value map[string]interface{}) {
	brokerId, _ := value["BrokerId"].(float64)
	broker := fmt.Sprintf("%v", brokerId)
	switch keys["type"] {
	case "app-info":
		kafkaVersion(broker, value["Version"])
	case "socket-server-metrics":
		socketServerMetrics(broker, keys, value)
	case "BrokerTopicMetrics":
		brokerTopicMetrics(broker, keys, value)
	case "ReplicaManager":
		replicaManager(broker, keys, value)
	}
}

func storeLogOffset(keys map[string]string, value map[string]interface{}) {
	brokerId, _ := value["BrokerId"].(float64)
	v, ok := value["Value"].(float64)
	if !ok {
		fmt.Printf("Cast failed for %v to float64\n", value["Value"])
		return
	}
	data := store.Data{
		Id: map[string]string{
			"metric":    keys["name"],
			"broker":    fmt.Sprintf("%v", brokerId),
			"topic":     keys["topic"],
			"partition": keys["partition"],
		},
		Value: int(v),
	}
	store.Put(data, []string{"metric", "topic", "partition"})
}

func kafkaVersion(broker string, version interface{}) {
	if val, ok := version.(string); ok {
		store.KafkaVersion[broker] = val
	}
}

func socketServerMetrics(broker string, keys map[string]string, value map[string]interface{}) {
	if keys["listener"] == "" {
		return
	}
	id := map[string]string{
		"metric":            "socket-server",
		"broker":            broker,
		"listener":          keys["listener"],
		"network_processor": keys["networkProcessor"],
	}
	index := []string{"metric", "broker"}
	for _, attr := range []string{"connection-count", "failed-authentication-total"} {
		id["attr"] = attr
		val, _ := value[attr].(float64)
		data := store.Data{Id: id, Value: int(val)}
		store.Put(data, index)
	}
}

func brokerTopicMetrics(broker string, keys map[string]string, value map[string]interface{}) {
	topic := keys["topic"]
	val, _ := value["OneMinuteRate"].(float64)
	id := map[string]string{"metric": keys["name"], "broker": broker}
	index := []string{"metric", "broker"}
	index = append(index, "broker")
	if topic != "" {
		id["topic"] = topic
		index = append(index, "topic")
	}
	data := store.Data{Id: id, Value: int(val)}
	store.Put(data, index)
}

func replicaManager(broker string, keys map[string]string, value map[string]interface{}) {
	id := map[string]string{"metric": keys["name"], "broker": broker}
	index := []string{"metric", "broker"}
	if val, ok := value["OneMinuteRate"]; ok {
		data := store.Data{Id: id, Value: int(val.(float64))}
		store.Put(data, index)
	} else if val, ok := value["Value"]; ok {
		data := store.Data{Id: id, Value: int(val.(float64))}
		store.Put(data, index)
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
