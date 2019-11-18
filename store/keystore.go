package store

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/cloudkarafka/cloudkarafka-manager/certs"
	"github.com/cloudkarafka/cloudkarafka-manager/config"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func getKeystore(storetype string) (certs.JKS, error) {
	if storetype == "truststore" {
		kafkaConfig, err := config.GetLocalKafkaConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Could not get kafka config: %s\n", err.Error())
			return certs.JKS{}, errors.New("Could not get kafka config")
		}
		path := kafkaConfig["ssl.truststore.location"]
		if path == "" {
			return certs.JKS{}, errors.New("Could not find ssl.truststore.location in kafka config")
		}
		if !strings.HasPrefix(path, "/") {
			path = filepath.Join(config.KafkaDir, path)
		}
		password := kafkaConfig["ssl.truststore.password"]
		storeType := kafkaConfig["ssl.truststore.type"]
		if storeType == "" {
			storeType = "JKS"
		}
		return certs.JKS{path, password, storeType}, nil
	}
	if _, err := os.Stat(".keystorepassword"); os.IsNotExist(err) {
		ioutil.WriteFile(".keystorepassword", []byte(randString(32)), 0400)
	}
	b, err := ioutil.ReadFile(".keystorepassword")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not read keystore password: %s\n", err.Error())
		return certs.JKS{}, errors.New("Could not read keystore password")
	}
	return certs.JKS{"clientkey.store", string(b), "PKCS12"}, nil
}
func GetKeystore(storetype string) (certs.JKS, error) {
	return getKeystore(storetype)
}

func listEntities(storeName string) ([]certs.StoreEntity, error) {
	var (
		err   error
		store certs.JKS
		res   []certs.StoreEntity
	)
	store, err = getKeystore(storeName)
	if err != nil {
		return nil, err
	}
	if store.Exists() {
		res, err = store.List()
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return res, nil
}

func TruststoreEntries() ([]certs.StoreEntity, error) {
	return listEntities("truststore")
}

func KeystoreEntries() ([]certs.StoreEntity, error) {
	return listEntities("keystore")
}
