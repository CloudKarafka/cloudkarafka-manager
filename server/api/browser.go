package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

type MessageData struct {
	Message   string `json:"message"`
	Key       string `json:"key"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    string `json:"offset"`
}

func consume(config *kafka.ConfigMap, topic string) (*kafka.Consumer, error) {
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %s", err)
		return nil, fmt.Errorf("Could not connect to Kafka: %s", err)
	}
	metadata, err := consumer.GetMetadata(&topic, false, 1000)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %s", err)
		return nil, fmt.Errorf("Could not get cluster metadata: %s", err)
	}
	parts := metadata.Topics[topic].Partitions
	toppar := make([]kafka.TopicPartition, len(parts))
	for i, p := range parts {
		toppar[i] = kafka.TopicPartition{Topic: &topic, Partition: p.ID, Offset: kafka.OffsetEnd}
	}
	err = consumer.Assign(toppar)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %s", err)
		return nil, fmt.Errorf("Could not assign topic partitions to consumer: %s", err)
	}
	return consumer, nil
}

func Browser(rw http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	vars := mux.Vars(r)
	if vars["topic"] == "" {
		http.Error(rw, "Missing topic", http.StatusInternalServerError)
		return
	}
	flusher, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusBadRequest)
		return
	}
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprint(rw, "\n")
	fmt.Fprint(rw, ":\n")
	fmt.Fprint(rw, "retry: 15000\n\n")

	d, _ := json.Marshal(map[string]interface{}{
		"message": "Starting consumer",
	})
	fmt.Fprintf(rw, "data: %s\n\n", d)
	flusher.Flush()

	config := &kafka.ConfigMap{
		"metadata.broker.list":       config.KafkaURL,
		"group.id":                   "kafka-browser",
		"enable.auto.commit":         false,
		"enable.auto.offset.store":   false,
		"go.events.channel.enable":   true,
		"queued.max.messages.kbytes": 512,
		"fetch.message.max.bytes":    5120,
		"queued.min.messages":        10,
	}
	consumer, err := consume(config, vars["topic"])
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	d, _ = json.Marshal(map[string]interface{}{
		"message": "Consuming",
	})
	fmt.Fprintf(rw, "data: %s\n\n", d)
	flusher.Flush()
	notify := rw.(http.CloseNotifier).CloseNotify()
	for {
		select {
		case <-notify:
			consumer.Close()
			return
		case <-time.After(10 * time.Second):
			fmt.Fprint(rw, ":\n\n")
			flusher.Flush()
		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				msg := map[string]interface{}{
					"message":   string(e.Value),
					"key":       string(e.Key),
					"partition": e.TopicPartition.Partition,
					"offset":    e.TopicPartition.Offset.String(),
					"timestamp": e.Timestamp,
					"headers":   []string{},
				}
				if e.Headers != nil {
					for _, h := range e.Headers {
						msg["headers"] = append(msg["headers"].([]string), h.String())
					}
				}
				json, _ := json.Marshal(msg)
				fmt.Fprintf(rw, "data: %s\n\n", json)
				flusher.Flush()
			case kafka.Error:
				switch e.Code() {
				case kafka.ErrNotImplemented:
				default:
					fmt.Fprintf(os.Stderr, "[ERROR] Kafka error: %s", e)
					fmt.Fprint(rw, "event: err\n")
					json, _ := json.Marshal(map[string]interface{}{
						"message": e.Error(),
					})
					fmt.Fprintf(rw, "data: %s\n\n", json)
					flusher.Flush()
					consumer.Close()
					return
				}
			}
		}
	}
}
