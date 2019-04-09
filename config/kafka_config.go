package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
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
