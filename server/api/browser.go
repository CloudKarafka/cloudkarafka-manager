package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"

	"github.com/segmentio/kafka-go"
	"goji.io/pat"
)

type MessageData struct {
	Message   string `json:"message"`
	Key       string `json:"key"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    string `json:"offset"`
}

func formatter(f string, b []byte) interface{} {
	switch f {
	case "byte-array":
		return b
	default:
		// string and json is by default a string
		return string(b)
	}
}

func reader(ctx context.Context, topic string, partition int, fanIn chan interface{}) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.BrokerUrls.List(),
		Topic:       topic,
		StartOffset: kafka.LastOffset,
		Partition:   partition,
	})
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			fanIn <- err
			break
		}
		fanIn <- msg
	}
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

	SetupSSE(15000, rw)

	fmt.Fprintf(os.Stdout, "[INFO] %s Topic browser: '%s'\n", rid, name)

	fanIn := make(chan interface{})
	t, _ := zookeeper.Topic(name)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for partition, _ := range t.Partitions {
		p, _ := strconv.Atoi(partition)
		go reader(ctx, name, p, fanIn)
	}
	notify := rw.(http.CloseNotifier).CloseNotify()
	for {
		select {
		case <-notify:
			fmt.Fprintf(os.Stdout, "[INFO] %s Topic browser: client closed\n", rid)
			return
		case <-time.After(10 * time.Second):
			fmt.Fprint(rw, ":\n\n")
			flusher.Flush()
		case ev := <-fanIn:
			switch e := ev.(type) {
			case kafka.Message:
				headers := make(map[string]string)
				for _, h := range e.Headers {
					headers[h.Key] = string(h.Value)
				}
				msg := map[string]interface{}{
					"message":   formatter(q.Get("vf"), e.Value),
					"key":       formatter(q.Get("kf"), e.Key),
					"partition": e.Partition,
					"offset":    e.Offset,
					"timestamp": e.Time,
					"headers":   headers,
				}
				json, _ := json.Marshal(msg)
				fmt.Fprintf(rw, "data: %s\n\n", json)
				flusher.Flush()
			case error:
				fmt.Fprintf(os.Stdout, "[INFO] Kafka browser: %s", e)
				fmt.Fprint(rw, "event: err\n")
				json, _ := json.Marshal(map[string]interface{}{
					"message": e.Error(),
				})
				fmt.Fprintf(rw, "data: %s\n\n", json)
				flusher.Flush()
				return
			}
		}
	}
}
