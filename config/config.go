package config

import "fmt"

var (
	KafkaURL  string
	Port      string
	Retention int64
	AuthType  string
	Version   string = "dev"
	GitCommit string = "master"
)

func PrintConfig() {
	fmt.Printf("Build info\n Version:\t%s\n Git commit:\t%s\n", Version, GitCommit)
	fmt.Printf("Runtime\n HTTP Port:\t%s\n Kafka host:\t%s\n Auth type:\t%s\n Retention:\t%d hours\n",
		Port, KafkaURL, AuthType, Retention)

}
