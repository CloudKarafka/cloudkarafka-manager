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

type client struct {
	Id         string `json:"id"`
	ConsumerId string `json:"consumer_id"`
	Host       string `json:"host"`
}

type ConsumerGroups map[string][]ConsumedPartition

type ConsumedPartition struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	CurrentOffset int    `json:"current_offset"`
	LogEndOffset  int    `json:"log_end_offset"`
	ClientId      string `json:"clientid"`
	ConsumerId    string `json:"consumerid"`
	Host          string `json:"host"`
	LastSeen      int64  `json:"last_seen"`
}

type ConsumerGroup struct {
	Name               string                   `json:"name"`
	Topics             []map[string]interface{} `json:"topics"`
	Clients            []client                 `json:"clients"`
	Online             bool                     `json:"online"`
	ConsumedPartitions []ConsumedPartition      `json:"consumed_partitions"`
	LastSeen           int64                    `json:"last_seen"`
}

func (g ConsumedPartition) Lag() int {
	return g.LogEndOffset - g.CurrentOffset
}

func (g ConsumerGroups) Lag(group string) map[string]int {
	res := make(map[string]int)
	for _, cp := range g[group] {
		res[cp.Topic] += cp.Lag()
	}
	return res
}

func (g ConsumerGroups) Topics(group string) []map[string]interface{} {
	topicMap := make(map[string]map[string]interface{})
	for _, member := range g[group] {
		t, ok := topicMap[member.Topic]
		if ok {
			lag := t["lag"].(int)
			t["lag"] = lag + member.Lag()
			clients := t["clients"].(map[string]struct{})
			clients[member.ConsumerId] = struct{}{}
		} else {
			t = map[string]interface{}{
				"lag": member.Lag(),
				"clients": map[string]struct{}{
					member.ConsumerId: struct{}{},
				},
			}
		}
		topicMap[member.Topic] = t
	}
	var (
		topics = make([]map[string]interface{}, len(topicMap))
		i      = 0
	)
	for name, t := range topicMap {
		t["name"] = name
		t["clients"] = len(t["clients"].(map[string]struct{}))
		topics[i] = t
		i += 1
	}
	return topics
}

func (g ConsumerGroups) Clients(group string) []client {
	var (
		clientMap = make(map[string]ConsumedPartition)
	)
	for _, member := range g[group] {
		clientMap[member.ConsumerId] = member
	}
	var (
		clients = make([]client, len(clientMap))
		i       = 0
	)
	for _, c := range clientMap {
		clients[i] = client{
			Id:         c.ClientId,
			ConsumerId: c.ConsumerId,
			Host:       c.Host,
		}
		i += 1
	}
	return clients
}

func (g ConsumerGroups) NumberConsumers(group string) int {
	var (
		groupMap = make(map[string]struct{})
		now      = time.Now().Unix()
	)
	for _, member := range g[group] {
		if now-member.LastSeen < 5 {
			groupMap[member.ConsumerId] = struct{}{}
		}
	}
	return len(groupMap)
}

func (g ConsumerGroups) Online(group string) bool {
	return g.NumberConsumers(group) > 0
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
			"online":  g.Online(group),
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
