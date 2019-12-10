package store

import (
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const MaxPoints int = 500

var brokerRequests []MetricRequest

func FetchMetrics(metrics chan Metric, reqs []MetricRequest) {
	for _, r := range reqs {
		resp, err := GetMetrics(r)
		if err != nil {
			log.Error("timeserie_getdata", log.ErrorEntry{err})
			continue
		}
		for _, r := range resp {
			metrics <- r
		}
	}
}

func Start() {
	brokerChanges := zookeeper.WatchBrokers()
	topicChanges := zookeeper.WatchTopics()
	ticker := time.NewTicker(time.Duration(5) * time.Second)
	defer ticker.Stop()
	metrics := make(chan Metric)
	for {
		select {
		case <-ticker.C:
			go FetchMetrics(metrics, brokerRequests)
		case hps := <-brokerChanges:
			brokerRequests = make([]MetricRequest, len(hps)*4)
			for i, hp := range hps {
				copy(brokerRequests[i*4:], []MetricRequest{
					MetricRequest{hp.Id, BeanAllTopicsBytesInPerSec, "Count"},
					MetricRequest{hp.Id, BeanAllTopicsBytesOutPerSec, "Count"},
					MetricRequest{hp.Id, BeanBrokerBytesInPerSec, "Count"},
					MetricRequest{hp.Id, BeanBrokerBytesOutPerSec, "Count"},
				})
				broker, _ := fetchBroker(hp.Id)
				store.UpdateBroker(broker)
			}
		case topics := <-topicChanges:
			for _, topic := range topics {
				topic, err := fetchTopic(topic.Name)
				if err != nil {
					continue
				}
				store.UpdateTopic(topic)
			}
		case metric := <-metrics:
			var key SerieKey
			if metric.Topic == "" {
				key = SerieKey{"broker", metric.Broker, "", metric.Name}
			} else {
				key = SerieKey{"topic", metric.Broker, metric.Topic, metric.Name}
			}
			if _, ok := Series[key]; !ok {
				Series[key] = NewSimpleTimeSerie(5, MaxPoints)
			}
			Series[key].(*SimpleTimeSerie).Add(int(metric.Value))
		}
	}
}

func adminClient() (*kafka.AdminClient, error) {
	adminConfig := &kafka.ConfigMap{"bootstrap.servers": strings.Join(config.BrokerUrls.List(), ",")}
	return kafka.NewAdminClient(adminConfig)
}
