package config

import (
	"flag"
	"strings"
	"time"
)

var (
	port           = flag.String("port", "8080", "Port to run HTTP server on")
	auth           = flag.String("authentication", "scram", "Valid values are scram, admin or dev")
	retention      = flag.Int("retention", 12, "Retention period (in hours) for historic data, set to 0 to disable history")
	requestTimeout = flag.Int("request-timeout", 5000, "Timeout in ms for requests to brokers to fetch metrics")
	zk             = flag.String("zookeeper", "localhost:2181", "The connection string for the zookeeper connection in the form host:port. Multiple hosts can be given to allow fail-over.")
	kafkaDir       = flag.String("kafkadir", "/opt/kafka", "The directory where kafka lives")
	devMode        = flag.Bool("dev", false, "Devmode add more logging and reloadable assets")
	noConsumers    = flag.Bool("no-consumers", false, "Disable listing of consumer groups")
	verbose        = flag.Bool("verbose", false, "Verbose logging")
)

func Parse() {
	flag.Parse()
	Retention = int64(*retention)
	Port = *port
	AuthType = *auth
	JMXRequestTimeout = time.Duration(*requestTimeout) * time.Millisecond
	KafkaDir = *kafkaDir
	ZookeeperURL = strings.Split(*zk, ",")
	DevMode = *devMode
	NoConsumers = *noConsumers
	VerboseLogging = *verbose
	if DevMode {
		VerboseLogging = true
	}
	PrintConfig()
}
