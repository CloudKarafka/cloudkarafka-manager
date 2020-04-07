package notifications

import (
	"fmt"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
)

func CheckPluginVersion(ch chan []Notification) {
	nots := make([]Notification, 0)
	for brokerId, _ := range config.BrokerUrls {
		res, err := store.PluginVersion(brokerId)
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
