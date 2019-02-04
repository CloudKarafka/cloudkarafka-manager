package notifications

import (
	"fmt"
	"os"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/metrics"
)

func buildNotification(brokerId int, level Level, key, title, message string) Notification {
	return Notification{
		Key:       fmt.Sprintf("%s-%d", key, brokerId),
		Title:     title,
		Level:     level,
		Message:   message,
		Timestamp: time.Now(),
		Link: NotificationLink{
			fmt.Sprintf("broker/details.html?id=%d", brokerId),
			"Go to broker"},
	}
}
func buildURPNotification(m metrics.Metric) Notification {
	return buildNotification(
		m.Broker,
		WARNING,
		"URP",
		"Under replicated partitions",
		fmt.Sprintf("Broker %d has %d under replicated paritions", m.Broker, int(m.Value)))
}

func CheckURP(out chan []Notification) {
	res := make([]Notification, 0)
	for brokerId, _ := range config.BrokerUrls {
		r, err := metrics.QueryBroker(brokerId, "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions", "Value", "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckURP: %s\n", err)
		} else {
			if r[0].Value > 0 {
				res = append(res, buildURPNotification(r[0]))
			}
		}
	}
	out <- res
}

func CheckBalancedLeaders(out chan []Notification) {
	res := make([]Notification, 0)
	stat := make(map[int]int)
	total := 0
	if len(config.BrokerUrls) == 1 {
		out <- res
		return
	}
	for brokerId, _ := range config.BrokerUrls {
		r, err := metrics.QueryBroker(brokerId, "kafka.server:type=ReplicaManager,name=LeaderCount", "Value", "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckBalancedLeader: %s\n", err)
		} else {
			stat[brokerId] = int(r[0].Value)
			total += int(r[0].Value)
		}
	}
	for b, s := range stat {
		percent := float64(s) / float64(total)
		if percent > float64(0.8) {
			res = append(res, buildNotification(
				b,
				WARNING,
				"LEAD_BALANCE",
				"Unbalanced leaders",
				fmt.Sprintf("Broker %d is leader for %.0f%% of partitions", b, 100*percent)))
		}
	}
	out <- res
}

func CheckISRDelta(out chan []Notification) {
	res := make([]Notification, 0)
	stat := make(map[int]int)
	for brokerId, _ := range config.BrokerUrls {
		r1, err1 := metrics.QueryBroker(brokerId, "kafka.server:type=ReplicaManager,name=IsrShrinksPerSec", "OneMinuteRate", "")
		r2, err2 := metrics.QueryBroker(brokerId, "kafka.server:type=ReplicaManager,name=IsrExpandsPerSec", "OneMinuteRate", "")
		if err1 != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckISRDelta: %s\n", err1)
		} else if err1 != nil {
			fmt.Fprintf(os.Stderr, "[INFO] CheckISRDelta: %s\n", err2)
		} else {
			stat[brokerId] = int(r2[0].Value) - int(r1[0].Value)
		}
	}
	for b, s := range stat {
		if s > 0 {
			res = append(res, buildNotification(
				b,
				WARNING,
				"ISR_DELTA",
				"ISR Delta",
				fmt.Sprintf("Broker %d is expanding and shrinking the in sync replicas frequently", b)))
		}
	}
	out <- res
}
