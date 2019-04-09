package config

import (
	"fmt"
	"math/rand"
	"time"
)

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
	Version           string = "dev"
	GitCommit         string = "master"
	JMXRequestTimeout time.Duration
	KafkaDir          string
)

func PrintConfig() {
	fmt.Printf("Build info\n Version:\t%s\n Git commit:\t%s\n", Version, GitCommit)
	fmt.Printf("Runtime\n HTTP Port:\t%s\n Auth type:\t%s\n Retention:\t%d hours\n",
		Port, AuthType, Retention)

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
