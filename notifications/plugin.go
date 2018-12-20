package notifications

import (
	"fmt"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/metrics"
)

func CheckPluginVersion(ch chan []Notification) {
	nots := make([]Notification, 0)
	for brokerId, _ := range metrics.BrokerUrls {
		res, err := metrics.PluginVersion(brokerId)
		if err != nil || res == "" {
			nots = append(nots, Notification{
				Key:       fmt.Sprintf("plugin-version-%d", brokerId),
				Title:     fmt.Sprintf("Metrics plugin not available on broker %d", brokerId),
				Level:     INFO,
				Message:   fmt.Sprintf("The metrics plugin is currently not available on broker %d, it was added as a plugin to the broker but the broker needs to be restarted in order for the plugin to activate. Please restart the broker. ", brokerId),
				Timestamp: time.Now(),
			})
		}
	}
	ch <- nots
}
