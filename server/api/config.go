package api

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/processes"
	"goji.io/pat"
)

func badRequest(w http.ResponseWriter, err error, fn string) {
	fmt.Fprintf(os.Stderr, "[ERROR] api.%s: %s", fn, err)
	http.Error(w, "Cannot parse request body", http.StatusBadRequest)
}

func serverError(w http.ResponseWriter, err error, fn, msg string) {
	fmt.Fprintf(os.Stderr, "[ERROR] api.%s: %s", fn, err)
	http.Error(w, msg, http.StatusBadRequest)
}

// Update kafka config on all brokers, rolling
func UpdateKafkaConfigAll(w http.ResponseWriter, r *http.Request) {
	var changes map[string]string
	err := parseRequestBody(r, changes)
	if err != nil {
		badRequest(w, err, "UpdateKafkaConfigAll")
		return
	}
	changesToApply := make(map[string]string)
	waitForURP := false
	for key, value := range changes {
		if config.IsDynamicClusterSetting(key) {
			changesToApply[key] = value
		} else if !config.IsDynamicBrokerSetting(key) {
			// If any change require kafka restart, wait for URP
			waitForURP = true
		}
	}
	if err := config.ReloadClusterConfig(changesToApply); err != nil {
		serverError(w, err, "UpdateKafkaConfigAll",
			fmt.Sprintf("Couldn't reload cluster-wide changed", err))
		return
	}
	for brokerId, _ := range config.BrokerUrls {
		fmt.Fprintf(os.Stderr, "[INFO] Starting config update on broker %d\n", brokerId)
		body := bytes.NewReader([]byte("")) // changes to json

		// Post changes to next mgmt
		http.Post(fmt.Sprintf("%s/api/config/%d/kafka", "IP", brokerId), "application/json", body)
		fmt.Fprintf(os.Stderr, "[INFO] Completed config update on broker %d\n", brokerId)
		if waitForURP {
			fmt.Fprintf(os.Stderr, "[INFO] Kafka was restarted on %d, waiting for URP\n", brokerId)
			// Check URP
			fmt.Fprintf(os.Stderr, "[INFO] URP status: broker=%d, URP=%d\n", brokerId, 0)
		}
	}
}

// Update kafka config on one broker
func UpdateKafkaConfig(w http.ResponseWriter, r *http.Request) {
	var changes map[string]string
	brokerId, err := strconv.Atoi(pat.Param(r, "brokerId"))
	if err != nil {
		badRequest(w, err, "UpdateKafkaConfig")
		return
	}
	err = parseRequestBody(r, changes)
	if err != nil {
		badRequest(w, err, "UpdateKafkaConfig")
		return
	}
	// Update Kafka config file with all changes
	if err = config.UpdateKafkaConfigFile(changes); err != nil {
		serverError(w, err, "UpdateKafkaConfig",
			fmt.Sprintf("Couldn't write changes to config file", err))
		return
	}
	changesToApply := make(map[string]string)
	restartKafka := false
	for key, value := range changes {
		if config.IsDynamicBrokerSetting(key) {
			changesToApply[key] = value
		} else if !config.IsDynamicClusterSetting(key) {
			// If change is not cluster nor broker dynamic, kafka must be restarted
			restartKafka = true
		}
	}
	if err = config.ReloadBrokerConfig(brokerId, changesToApply); err != nil {
		serverError(w, err, "UpdateKafkaConfig",
			"Couldn't reload kafka config but changes are written to config file")
	}
	if restartKafka {
		if err = processes.RestartKafka(); err != nil {
			serverError(w, err, "UpdateKafkaConfig",
				"Couldm't restart kafka, only dynamic changes were applied")
		}
	}
}
