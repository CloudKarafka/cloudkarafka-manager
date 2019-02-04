package api

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	m "github.com/cloudkarafka/cloudkarafka-manager/metrics"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

func topicOverview(ctx context.Context, p zookeeper.Permissions, res map[string]int) map[string]int {
	res["topic_size"] = 0
	res["topic_msg_count"] = 0
	res["topic_count"] = 0
	topics, err := m.FetchTopicList(ctx, p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] api.topicOverview: %s", err)
		return res
	}
	for _, topic := range topics {
		res["topic_size"] += topic.Size()
		res["topic_msg_count"] += topic.Messages()
		res["topic_count"] += 1
	}
	return res
}
func brokerOverview(res map[string]int) map[string]int {
	res["broker_count"] = len(config.BrokerUrls)
	if brokers, err := zookeeper.Brokers(); err == nil {
		res["online_broker_count"] = len(brokers)
	}
	return res
}

// TODO Show only consumer that consumes from topics that user has permissions for?
func consumerOverview(ctx context.Context, res map[string]int) map[string]int {
	res["consumer_count"] = 0
	if v, err := m.FetchConsumerGroups(ctx); err == nil {
		res["consumer_count"] = len(v)
	}
	return res
}

func userOverview(p zookeeper.Permissions, res map[string]int) map[string]int {
	res["user_count"] = 0
	users, err := zookeeper.Users(p)
	if err != nil {
		return res
	}
	res["user_count"] = len(users)
	return res
}

func Overview(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	res := make(map[string]int)
	res = topicOverview(ctx, p, res)
	res = brokerOverview(res)
	res = consumerOverview(ctx, res)
	res = userOverview(p, res)
	writeAsJson(w, res)
}
