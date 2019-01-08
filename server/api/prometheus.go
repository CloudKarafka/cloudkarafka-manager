package api

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/metrics"
)

type PromMetric struct {
	bean  string
	attrs []string
	name  string
}

var promMetrics = []PromMetric{
	PromMetric{"kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions", []string{"Value"}, "kafka_broker_urp"},
}

func check(wg *sync.WaitGroup, m PromMetric, out chan string) {
	defer wg.Done()
	for brokerId, _ := range config.BrokerUrls {
		r, err := metrics.QueryBroker(brokerId, m.bean, strings.Join(m.attrs, ","), "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] Prom.check: %s\n", err)
		} else {
			out <- fmt.Sprintf("%s{broker=%d,attr=%s} %d", m.name, brokerId, r[0].Attribute, int(r[0].Value))
		}
	}

}

func Prometheus(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var wg sync.WaitGroup
	ch := make(chan string)
	wg.Add(len(promMetrics))
	for _, m := range promMetrics {
		go check(&wg, m, ch)
	}
	go func() {
		for v := range ch {
			w.Write([]byte(v + "\n"))
		}
	}()
	wg.Wait()
	time.Sleep(1)
	fmt.Fprintf(os.Stderr, "Prometheus took %.4fs\n", time.Since(start).Seconds())
	close(ch)
}
