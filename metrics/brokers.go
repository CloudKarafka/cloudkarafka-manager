package metrics

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/db"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	humanize "github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
)

type Broker struct {
	Version      int      `json:"version"`
	JmxPort      int      `json:"jmx_port"`
	Timestamp    string   `json:"timestamp"`
	Uptime       string   `json:"uptime"`
	Online       bool     `json:"online"`
	Endpoints    []string `json:"endpoints"`
	Host         string   `json:"host"`
	Port         int      `json:"port"`
	Id           int      `json:"id"`
	KafkaVersion string   `json:"kafka_version"`
}

func FetchBrokerMetrics(brokerId int, detailed bool) (TopicMetrics, error) {
	queries := [][]string{
		[]string{"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "OneMinuteRate"},
		[]string{"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "OneMinuteRate"},
		[]string{"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "OneMinuteRate"},
		[]string{"kafka.controller:type=KafkaController,name=ActiveControllerCount", "Value"},
	}
	if detailed {
		queries = append(queries, [][]string{
			[]string{"kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec", "OneMinuteRate"},
			[]string{"kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions", "Value"},
			[]string{"kafka.controller:type=KafkaController,name=OfflinePartitionsCount", "Value"},
			[]string{"kafka.server:type=ReplicaManager,name=PartitionCount", "Value"},
			[]string{"kafka.server:type=ReplicaManager,name=LeaderCount", "Value"},
		}...)
	}
	metrics := make(TopicMetrics)
	l := len(queries)
	ch := make(chan []Metric, l)
	for _, query := range queries {
		go QueryBrokerAsync(brokerId, query[0], query[1], ch)
	}
	timeout := time.After(80 * time.Millisecond)
	for i := 0; i < l; i++ {
		select {
		case ress := <-ch:
			for _, res := range ress {
				metrics[res.Name] = append(metrics[res.Name], TopicMetricValue{res.Broker, int(res.Value)})
			}

		case <-timeout:
			fmt.Fprintf(os.Stderr, "[INFO] Topic metrics request timed out\n")
			return metrics, RequestTimedOutErr
		}
	}
	return metrics, nil
}

func FetchBrokerConnections(brokerId int) (TopicMetrics, error) {
	bean := "kafka.server:type=socket-server-metrics,listener=*,networkProcessor=*"
	res, err := QueryBroker(brokerId, bean, "connection-count", "listener")
	if err != nil {
		return nil, err
	}
	metrics := make(TopicMetrics)
	for _, m := range res {
		metrics[m.Listener] = append(metrics[m.Listener], TopicMetricValue{m.Broker, int(m.Value)})
	}
	return metrics, nil

}

func FetchBroker(id int) (Broker, error) {
	var broker Broker
	path := fmt.Sprintf("/brokers/ids/%d", id)
	err := zookeeper.Get(path, &broker)
	if err != nil {
		return broker, err
	}
	broker.Id = id
	broker.Online = true
	ts, err := strconv.ParseInt(broker.Timestamp, 10, 64)
	broker.Uptime = strings.TrimSpace(humanize.RelTime(time.Now(), time.Unix(ts/1000, 0), "", ""))
	if version, err := KafkaVersion(id); err == nil {
		broker.KafkaVersion = version
	}
	return broker, nil
}

func BrokersThroughput(metrics map[string]string, brokerIds []int, from time.Time) ([]*db.Serie, error) {
	var res []*db.Serie
	err := db.View(func(tx *bolt.Tx) error {
		for k, name := range metrics {
			keySplit := strings.Split(k, "_")
			s := &db.Serie{Name: name, Type: keySplit[0]}
			for _, id := range brokerIds {
				path := fmt.Sprintf("broker_metrics/%d/%s", id, keySplit[1])
				d := db.TimeSerie(tx, path, from)
				s.Add(d)
			}
			res = append(res, s)
		}
		return nil
	})
	return res, err
}
