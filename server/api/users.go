package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cloudkarafka/cloudkarafka-manager/certs"
	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

type user struct {
	Name, Password string
}

func Users(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	users, err := zookeeper.Users(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] api.Users: %s", err)
		http.Error(w, "Could not retrive save user in ZooKeeper", http.StatusInternalServerError)
		return
	}
	res := make([]zookeeper.Permissions, len(users))
	for i, user := range users {
		res[i] = zookeeper.PermissionsFor(user)
	}
	writeAsJson(w, res)
}

func CreateUser(w http.ResponseWriter, r *http.Request) {
	form := make(map[string]string)
	err := parseRequestBody(r, &form)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateUser: %s", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	err = zookeeper.CreateUser(form["name"], form["password"])
	if err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	fmt.Printf("[INFO] action=create-user user=%s\n", form["name"])
	w.WriteHeader(http.StatusCreated)
}

func User(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	user := zookeeper.PermissionsFor(name)
	writeAsJson(w, user)
}

func DeleteUser(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	if name != "admin" {
		zookeeper.DeleteUser(name)
		fmt.Printf("[INFO] action=delete-user user=%s\n", name)
	}
	w.WriteHeader(http.StatusNoContent)
}

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
		kafkaConfig, err := config.GetKafkaConfig()
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
		if password == "" {
			return certs.JKS{}, errors.New("Could not find ssl.truststore.password in kafka config")
		}
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

func ListSSLCerts(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		truststore certs.JKS
		keystore   certs.JKS
		trusted    []certs.StoreEntity
		stored     []certs.StoreEntity
	)
	truststore, err = getKeystore("truststore")
	if err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, "Could not get path to trust store from the broker", http.StatusInternalServerError)
	}
	if truststore.Exists() {
		trusted, err = truststore.List()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Could not get certificates from trust store: %s\n", err.Error())
			w.Header().Add("Content-type", "text/plain")
			http.Error(w, "Could not read certificates from truststore", http.StatusInternalServerError)
			return
		}
	}
	keystore, err = getKeystore("keystore")
	if err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	if keystore.Exists() {
		stored, err = keystore.List()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Could not get certificates from keystore: %s\n", err.Error())
			w.Header().Add("Content-type", "text/plain")
			http.Error(w, "Could not read certificates from keystore", http.StatusInternalServerError)
			return
		}
	}
	writeAsJson(w, map[string][]certs.StoreEntity{
		"trusted": trusted,
		"stored":  stored,
	})
}

// Creates a new private key and certificate
// Adds the certificate to the truststore
// Adds the private key to the keystore, for renewing the cert in the future
func CreateSSLCert(w http.ResponseWriter, r *http.Request) {
	form := make(map[string]string)
	err := parseRequestBody(r, &form)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.GenerateSSLCert: %s", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	if form["validity"] == "" {
		form["validity"] = "365"
	}
	subject := certs.CertSubject{
		CountryCode: form["country"],
		State:       form["state"],
		City:        form["city"],
		Company:     form["company"],
		Section:     form["section"],
		CommonName:  form["commonname"]}
	pair, err := certs.GenerateCert("", subject, form["validity"])
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s\n", err)
		http.Error(w, "Failed to generate certificate", http.StatusInternalServerError)
		return
	}
	truststore, err := getKeystore("truststore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s\n", err)
		http.Error(w, "Failed to find the truststore", http.StatusInternalServerError)
		return
	}
	keystore, err := getKeystore("keystore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s\n", err)
		http.Error(w, "Failed to find the keystore", http.StatusInternalServerError)
		return
	}
	if err = truststore.ImportCert(pair.PublicKey, form["alias"]); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert Import cert failed: %s\n", err)
		http.Error(w, "Failed to store the certificate in the truststore", http.StatusInternalServerError)
		return
	}
	if err = keystore.ImportPrivateKey(pair, form["alias"]); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert Import private key failed: %s\n", err)
		http.Error(w, "Failed to store the key in the keystore", http.StatusInternalServerError)
		return
	}
	changes := map[string]string{"listener.name.ssl.ssl.truststore.location": truststore.Path}
	if err := config.ReloadConfigValueAllBrokers(changes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(pair.PrivateKey))
	w.Write([]byte("\n"))
	w.Write([]byte(pair.PublicKey))
}

// Import cert generated somewhere else into the truststore
func ImportSSLCert(w http.ResponseWriter, r *http.Request) {
	alias := pat.Param(r, "alias")
	defer r.Body.Close()
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.ImportSSLCert: %s", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	cert := string(bytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.ImportSSLCert: %s", err)
		http.Error(w, "Failed to find the truststore", http.StatusInternalServerError)
		return
	}
	valid, err := certs.ValidateCert(cert)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.ImportSSLCert: %s\n", err)
		http.Error(w, "Failed to validate certificate", http.StatusInternalServerError)
		return
	}
	if !valid {
		fmt.Fprintf(os.Stderr, "[ERROR] api.ImportSSLCert: %s", err)
		http.Error(w, "The certificate supplied is not a valid certificate", http.StatusBadRequest)
		return
	}
	truststore, err := getKeystore("truststore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s", err)
		http.Error(w, "Failed to find the keystore", http.StatusInternalServerError)
		return
	}

	if err = truststore.ImportCert(cert, alias); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.ImportSSLCert: %s", err)
		http.Error(w, "Failed to import certificate", http.StatusInternalServerError)
		return
	}
	changes := map[string]string{"listener.name.ssl.ssl.truststore.location": truststore.Path}
	if err := config.ReloadConfigValueAllBrokers(changes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Generate a new cert based on the stored private key (matching alias in keystore)
func RenewSSLCert(w http.ResponseWriter, r *http.Request) {
	alias := pat.Param(r, "alias")
	form := make(map[string]string)
	err := parseRequestBody(r, &form)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.GenerateSSLCert: %s\n", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	if form["validity"] == "" {
		form["validity"] = "365"
	}
	subject := certs.CertSubject{
		CountryCode: form["country"],
		State:       form["state"],
		City:        form["city"],
		Company:     form["company"],
		Section:     form["section"],
		CommonName:  form["commonname"]}
	keystore, err := getKeystore("keystore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RenewSSLCert: %s\n", err)
		http.Error(w, "Failed to find the keystore", http.StatusInternalServerError)
		return
	}
	// If no keystore exists means that no private keys are generated
	// Renewing a cert for a key that doesn't exists gives a 404
	if !keystore.Exists() {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	pair, err := keystore.RenewCert(alias, subject, form["validity"])
	if err != nil {
		if err == certs.NoAliasError {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			fmt.Fprintf(os.Stderr, "[ERROR] api.CreateRenewSSLCert: %s\n", err)
			http.Error(w, "Failed to renew the certificate", http.StatusInternalServerError)
		}
		return
	}
	truststore, err := getKeystore("truststore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RenewSSLCert: %s\n", err)
		http.Error(w, "Failed to find the truststore", http.StatusInternalServerError)
		return
	}
	if err := truststore.RemoveEntry(alias); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RenewSSLCert: %s", err)
		http.Error(w, "Failed to revoke old certificate", http.StatusInternalServerError)
		return
	}

	if err = truststore.ImportCert(pair.PublicKey, alias); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RenewSSLCert: %s\n", err)
		http.Error(w, "Failed to import renewed certificate", http.StatusInternalServerError)
		return
	}
	changes := map[string]string{"listener.name.ssl.ssl.truststore.location": truststore.Path}
	if err := config.ReloadConfigValueAllBrokers(changes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(pair.PublicKey))
}

// Revoke access to a certificate
func RevokeSSLCert(w http.ResponseWriter, r *http.Request) {
	alias := pat.Param(r, "alias")
	truststore, err := getKeystore("truststore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s", err)
		http.Error(w, "Failed to find the keystore", http.StatusInternalServerError)
		return
	}
	if err := truststore.RemoveEntry(alias); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.DeleteSSLCert: %s", err)
		http.Error(w, "Failed to revoke certificate", http.StatusInternalServerError)
		return
	}
	changes := map[string]string{"listener.name.ssl.ssl.truststore.location": truststore.Path}
	if err := config.ReloadConfigValueAllBrokers(changes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func RemoveSSLKey(w http.ResponseWriter, r *http.Request) {
	alias := pat.Param(r, "alias")
	store, err := getKeystore("keystore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RemoveSSLKey: %s", err)
		http.Error(w, "Failed to find the keystore", http.StatusInternalServerError)
		return
	}
	if err := store.RemoveEntry(alias); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RemoveSSLKey: %s", err)
		http.Error(w, "Failed to remove private key from keystore", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
