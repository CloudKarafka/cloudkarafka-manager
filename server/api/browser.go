package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"goji.io/pat"
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
		fmt.Fprintf(os.Stderr, "[INFO] Kafka browser: %s", err)
		return nil, fmt.Errorf("Could not connect to Kafka: %s", err)
	}
	metadata, err := consumer.GetMetadata(&topic, false, 1000)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] Kafka browser: %s", err)
		return nil, fmt.Errorf("Could not get cluster metadata: %s", err)
	}
	parts := metadata.Topics[topic].Partitions
	toppar := make([]kafka.TopicPartition, len(parts))
	for i, p := range parts {
		toppar[i] = kafka.TopicPartition{Topic: &topic, Partition: p.ID, Offset: kafka.OffsetEnd}
	}
	err = consumer.Assign(toppar)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] Kafka browser: %s", err)
		return nil, fmt.Errorf("Could not assign topic partitions to consumer: %s", err)
	}
	return consumer, nil
}

func formatter(f string, b []byte) interface{} {
	switch f {
	case "byte-array":
		return b
	}
	// string and json is by default a string
	return string(b)
}

func TopicBrowser(rw http.ResponseWriter, r *http.Request) {
	rid := r.Context().Value("requestId").(string)
	name := pat.Param(r, "name")
	q := r.URL.Query()
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

	fmt.Fprintf(os.Stderr, "[INFO] %s Topic browser: '%s'\n", rid, name)
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
	consumer, err := consume(config, name)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	notify := rw.(http.CloseNotifier).CloseNotify()
	for {
		select {
		case <-notify:
			fmt.Fprintf(os.Stderr, "[INFO] %s Topic browser: client closed\n", rid)
			consumer.Close()
			return
		case <-time.After(10 * time.Second):
			fmt.Fprint(rw, ":\n\n")
			flusher.Flush()
		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				msg := map[string]interface{}{
					"message":   formatter(q.Get("vf"), e.Value),
					"key":       formatter(q.Get("kf"), e.Key),
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
					fmt.Fprintf(os.Stderr, "[INFO] Kafka browser: %s", e)
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
