package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func getKafkaConfig(url string) (map[string]string, error) {
	res := make(map[string]string)
	r, err := http.Get(url)
	if err != nil {
		return res, err
	}
	if r.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "[INFO] GET %s returned %s\n", url, r.Status)
		return res, errors.New("Didn't recieve an OK respose from broker")
	}
	defer r.Body.Close()
	err = json.NewDecoder(r.Body).Decode(&res)
	return res, err
}

var dynamicBrokerSettings = []string{
	"listener.name.ssl.ssl.truststore.location",
}
var dynamicClusterSettings = []string{
	"min.insync.replicas",
}

func inList(list []string, key string) bool {
	for _, k := range list {
		if k == key {
			return true
		}
	}
	return false
}

func IsDynamicBrokerSetting(key string) bool {
	return inList(dynamicBrokerSettings, key)
}

func IsDynamicClusterSetting(key string) bool {
	return inList(dynamicClusterSettings, key)
}

// Assuming PLAINTEXT port on the broker is default, 9092
func GetLocalKafkaConfig() (map[string]string, error) {
	return getKafkaConfig("http://localhost:19092/config")
}

func GetKafkaConfig(brokerId int) (map[string]string, error) {
	return getKafkaConfig(BrokerUrls.HttpUrl(brokerId))
}

// TODO: No need to use kafka-config.sh, could write the changes directly to Zookeeper
func ReloadBrokerConfig(brokerId int, changes map[string]string) error {
	cfg := make([]string, len(changes))
	idx := 0
	for k, v := range changes {
		cfg[idx] = fmt.Sprintf("%s=%s", k, v)
		idx += 1
	}
	cmd := exec.Command(filepath.Join(KafkaDir, "bin/kafka-configs.sh"),
		"--bootstrap-server", strings.Join(BrokerUrls.List(), ","),
		"--entity-type", "brokers",
		"--entity-name", fmt.Sprintf("%d", brokerId),
		"--alter",
		"--add-config", strings.Join(cfg, ","))
	fmt.Println(cmd.Args)
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Reload broker config failed: %s\n%s", err, out)
		return fmt.Errorf("Failed to reload config on broker %d", brokerId)
	}
	return nil
}

// TODO: No need to use kafka-config.sh, could write the changes directly to Zookeeper
func ReloadClusterConfig(changes map[string]string) error {
	cfg := make([]string, len(changes))
	idx := 0
	for k, v := range changes {
		cfg[idx] = fmt.Sprintf("%s=%s", k, v)
		idx += 1
	}
	cmd := exec.Command(filepath.Join(KafkaDir, "bin/kafka-configs.sh"),
		"--bootstrap-server", strings.Join(BrokerUrls.List(), ","),
		"--entity-type", "brokers",
		"--entity-default",
		"--alter",
		"--add-config", strings.Join(cfg, ","))
	fmt.Println(cmd.Args)
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Reload cluster config failed: %s\n%s", err, out)
		return fmt.Errorf("Failed to reload config on cluster")
	}
	return nil
}

// Only reload the changes is possible, will not write changes to config file nor restart kafka
func ReloadConfigValueAllBrokers(changes map[string]string) error {
	clusterChanges := make(map[string]string)
	brokerChanges := make(map[string]string)
	for k, v := range changes {
		if IsDynamicClusterSetting(k) {
			clusterChanges[k] = v
		} else if IsDynamicBrokerSetting(k) {
			brokerChanges[k] = v
		}
	}
	if len(clusterChanges) > 0 {
		ReloadClusterConfig(clusterChanges)
	}
	if len(brokerChanges) > 0 {
		for brokerId, _ := range BrokerUrls {
			if err := ReloadBrokerConfig(brokerId, brokerChanges); err != nil {
				return err
			}
		}
	}
	return nil
}

func UpdateKafkaConfigFile(changes map[string]string) error {
	return nil
}
