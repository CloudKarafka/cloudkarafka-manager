package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
)

type ConsumerGroupMember struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	CurrentOffset int    `json:"current_offset"`
	LogEndOffset  int    `json:"log_end_offset"`
	ClientId      string `json:"clientid"`
	ConsumerId    string `json:"consumerid"`
	Host          string `json:"host"`
	LastSeen      int    `json:"last_seen"`
}

func (g ConsumerGroupMember) Lag() int {
	return g.LogEndOffset - g.CurrentOffset
}

type ConsumerGroups map[string][]ConsumerGroupMember

func (g ConsumerGroups) Lag(group string) int {
	res := 0
	for _, member := range g[group] {
		res += member.LogEndOffset - member.CurrentOffset
	}
	return res
}
func (g ConsumerGroups) Topics(group string) []string {
	groupMap := make(map[string]bool)
	for _, member := range g[group] {
		groupMap[member.Topic] = true
	}
	topics := make([]string, len(groupMap))
	i := 0
	for k, _ := range groupMap {
		topics[i] = k
		i += 1
	}
	return topics
}

func (g ConsumerGroups) NumberConsumers(group string) int {
	groupMap := make(map[string]bool)
	for _, member := range g[group] {
		groupMap[member.ConsumerId] = true
	}
	return len(groupMap)
}

func (g ConsumerGroups) MarshalJSON() ([]byte, error) {
	var (
		res = make([]map[string]interface{}, len(g))
		i   = 0
	)
	for group, _ := range g {
		res[i] = map[string]interface{}{
			"name":    group,
			"topics":  g.Topics(group),
			"lag":     g.Lag(group),
			"clients": g.NumberConsumers(group),
		}
		i += 1
	}
	return json.Marshal(res)
}

func FetchConsumerGroups(ctx context.Context, out chan ConsumerGroups) {
	var (
		err error
		v   ConsumerGroups
		r   *http.Response
	)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	host := config.BrokerUrls.Rand()
	if host == "" {
		log.Error("fetch_consumer_groups", log.StringEntry("No brokers to request consumer group metrics from"))
		return
	}
	url := fmt.Sprintf("%s/consumer-groups", config.BrokerUrls.Rand())
	select {
	case <-ctx.Done():
		log.Error("fetch_consumer_groups", log.ErrorEntry{ctx.Err()})
		return
	default:
		r, err = http.Get(url)
		if err != nil {
			log.Error("fetch_consumer_groups", log.ErrorEntry{err})
			return
		}
		err = json.NewDecoder(r.Body).Decode(&v)
		if err != nil {
			log.Error("fetch_consumer_groups", log.ErrorEntry{err})
			return
		}
		out <- v
	}
}
