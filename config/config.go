package config

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
)

func init() {
	ch := make(chan map[int]HostPort)
	BrokerChangeListeners = append(BrokerChangeListeners, ch)
	go func() {
		for v := range ch {
			le := make(log.MapEntry)
			for id, hostport := range v {
				le[strconv.Itoa(id)] = hostport
			}
			if len(le) > 0 {
				log.Info("broker_change", le)
			}
			BrokerUrls = v
		}
	}()
}

type HostPort struct {
	Host string
	Port int
}
type BrokerURLs map[int]HostPort

var (
	BrokerUrls        BrokerURLs
	Port              string
	Retention         int64
	AuthType          string
	BuildDate         string = time.Now().Format("2006-01-02")
	GitCommit         string = "master"
	JMXRequestTimeout time.Duration
	KafkaDir          string
	ZookeeperURL      []string
	WebRequestTimeout time.Duration = 5 * time.Second
	DevMode           bool          = false
)

func PrintConfig() {
	fmt.Printf("Build info\n BuildDate:\t%s\n Git commit:\t%s\n", BuildDate, GitCommit)
	fmt.Printf("Runtime\n HTTP Port:\t%s\n Auth type:\t%s\n Retention:\t%d hours\n",
		Port, AuthType, Retention)

}

func (b BrokerURLs) IDs() []int {
	brokerIds := make([]int, len(b))
	i := 0
	for id, _ := range b {
		brokerIds[i] = id
		i += 1
	}
	return brokerIds
}

func (b BrokerURLs) KafkaUrl(k int) string {
	if b[k].Host == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", b[k].Host, b[k].Port)
}

// Metrics reporter exposes http server on port 10000+PLAINTEXT-PORT (19092)
func (b BrokerURLs) HttpUrl(k int) string {
	if b[k].Host == "" {
		return ""
	}
	return fmt.Sprintf("http://%s:1%d", b[k].Host, b[k].Port)
}

// Metrics reporter exposes http server on port 10000+PLAINTEXT-PORT (19092)
func (b BrokerURLs) MgmtUrl(k int) string {
	if b[k].Host == "" {
		return ""
	}
	return fmt.Sprintf("http://%s:8080", b[k].Host)
}

func (b BrokerURLs) Rand() string {
	if len(b) == 0 {
		return ""
	}
	i := rand.Intn(len(b))
	var k int
	for k = range b {
		if i == 0 {
			break
		}
		i--
	}
	return b.HttpUrl(k)
}
func (b BrokerURLs) List() []string {
	var res []string
	for k := range b {
		if v := b.KafkaUrl(k); v != "" {
			res = append(res, b.KafkaUrl(k))
		}
	}
	return res
}
