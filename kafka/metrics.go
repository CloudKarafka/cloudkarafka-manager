package kafka

import (
	"cloudkarafka-mgmt/store"

	"github.com/Shopify/sarama"

	"encoding/json"
	"fmt"
	"strings"
)

func metricMessage(msg *sarama.ConsumerMessage) {
	mbeanName := strings.Split(string(msg.Key), ",")
	if len(mbeanName) == 0 {
		fmt.Println(string(msg.Key))
		return
	}
	keys := make(map[string]string)
	for _, m := range mbeanName {
		kv := strings.Split(m, "=")
		if len(kv) != 2 {
			fmt.Println(m)
			continue
		}
		keys[kv[0]] = kv[1]
	}
	value, err := parseJson(msg.Value)
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
	switch keys["type"] {
	case "app-info":
		store.KafkaVersion = value["Version"].(string)
	case "BrokerTopicMetrics":
		topic := keys["topic"]
		val, _ := value["OneMinuteRate"].(float64)
		id := map[string]string{"metric": keys["name"]}
		index := []string{"metric"}
		if topic == "" {
			brokerId := value["BrokerId"].(float64)
			id["broker"] = fmt.Sprintf("%v", brokerId)
			index = append(index, "broker")
		} else {
			id["topic"] = topic
			index = append(index, "topic")
		}
		data := store.Data{Id: id, Value: int(val)}
		store.Put(data, index)
	}
}

func storeLogOffset(keys map[string]string, value map[string]interface{}) {
	v, ok := value["Value"].(float64)
	if !ok {
		fmt.Printf("Cast failed for %v to float64\n", value["Value"])
		return
	}
	data := store.Data{
		Id: map[string]string{
			"metric":    keys["name"],
			"topic":     keys["topic"],
			"partition": keys["partition"],
		},
		Value: int(v),
	}
	store.Put(data, []string{"metric", "topic", "partition"})
}

func parseJson(bytes []byte) (map[string]interface{}, error) {
	value := make(map[string]interface{})
	err := json.Unmarshal(bytes, &value)
	if err != nil {
		return value, err
	}
	return value, nil
}
