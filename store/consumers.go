package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
)

type ConsumerGroupMember struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	CurrentOffset int    `json:"current_offset"`
	LogEndOffset  int    `json:"log_end_offset"`
	ClientId      string `json:"clientid"`
	ConsumerId    string `json:"consumerid"`
	Host          string `json:"host"`
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
	res := make(map[string]map[string]interface{})
	for group, _ := range g {
		res[group] = map[string]interface{}{
			"topics":  g.Topics(group),
			"lag":     g.Lag(group),
			"clients": g.NumberConsumers(group),
		}
	}
	return json.Marshal(res)
}

func FetchConsumerGroups(ctx context.Context) (ConsumerGroups, error) {
	var (
		err error
		v   ConsumerGroups
		r   *http.Response
	)
	host := config.BrokerUrls.Rand()
	if host == "" {
		return v, errors.New("No brokers to request consumer group metrics from")
	}
	url := fmt.Sprintf("%s/consumer-groups", config.BrokerUrls.Rand())
	r, err = http.Get(url)
	if err != nil {
		return v, err
	}
	err = json.NewDecoder(r.Body).Decode(&v)
	return v, err
}
