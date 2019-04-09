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

func GetKafkaConfig() (map[string]string, error) {
	res := make(map[string]string)
	url := BrokerUrls.HttpUrl(0) + "/config"
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

func ReloadConfigValue(broker string, changes map[string]string) error {
	cfg := make([]string, len(changes))
	idx := 0
	for k, v := range changes {
		cfg[idx] = fmt.Sprintf("%s=%s", k, v)
		idx += 1
	}
	cmd := exec.Command(filepath.Join(KafkaDir, "bin/kafka-configs.sh"),
		"--bootstrap-server", strings.Join(BrokerUrls.List(), ","),
		"--entity-type", "brokers",
		"--entity-name", broker,
		"--alter",
		"--add-config", strings.Join(cfg, ","))
	fmt.Println(cmd.Args)
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Reload config failed: %s\n%s", err, out)
		return fmt.Errorf("Failed to update config on broker %s", broker)
	}
	return nil
}
func ReloadConfigValueAllBrokers(changes map[string]string) error {
	for brokerId, _ := range BrokerUrls {
		if err := ReloadConfigValue(fmt.Sprintf("%d", brokerId), changes); err != nil {
			return err
		}
	}
	return nil
}
