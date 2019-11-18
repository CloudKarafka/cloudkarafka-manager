package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/cloudkarafka/cloudkarafka-manager/certs"
	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

type user struct {
	Name, Password string
}

func Users(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	username := r.Context().Value("username").(string)
	users, err := zookeeper.Users(username, p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] api.Users: %s", err)
		http.Error(w, "Could not retrive save user in ZooKeeper", http.StatusInternalServerError)
		return
	}
	/*
		res := make([]zookeeper.Permissions, len(users))
		for i, user := range users {
			res[i] = zookeeper.PermissionsFor(user)
		}
	*/
	writeAsJson(w, users)
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
	user, err := zookeeper.PermissionsFor(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.User: %s", err)
		http.Error(w, "Couldn't get info from zookeeper", http.StatusInternalServerError)
		return
	}
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

func ListSSLCerts(w http.ResponseWriter, r *http.Request) {
	var (
		err error

		trusted []certs.StoreEntity
		stored  []certs.StoreEntity
	)

	stored, err = store.KeystoreEntries()
	if err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	trusted, err = store.TruststoreEntries()
	if err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
	truststore, err := store.GetKeystore("truststore")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s\n", err)
		http.Error(w, "Failed to find the truststore", http.StatusInternalServerError)
		return
	}
	keystore, err := store.GetKeystore("keystore")
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
	if err := truststore.Distribute(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s", err)
		http.Error(w,
			fmt.Sprintf("Failed to distribute certificate to other brokers: %s", err),
			http.StatusInternalServerError)
		return
	}

	if err = keystore.ImportPrivateKey(pair, form["alias"]); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert Import private key failed: %s\n", err)
		http.Error(w, "Failed to store the key in the keystore", http.StatusInternalServerError)
		return
	}
	if err := keystore.Distribute(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateSSLCert: %s", err)
		http.Error(w,
			fmt.Sprintf("Failed to distribute client private key to other brokers: %s", err),
			http.StatusInternalServerError)
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
	truststore, err := store.GetKeystore("truststore")
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
	if err := truststore.Distribute(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.ImportSSLCert: %s", err)
		http.Error(w,
			fmt.Sprintf("Failed to distribute certificate to other brokers: %s", err),
			http.StatusInternalServerError)
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
	keystore, err := store.GetKeystore("keystore")
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
	truststore, err := store.GetKeystore("truststore")
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
	if err := truststore.Distribute(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RenewSSLCert: %s", err)
		http.Error(w,
			fmt.Sprintf("Failed to distribute certificate to other brokers: %s", err),
			http.StatusInternalServerError)
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
	truststore, err := store.GetKeystore("truststore")
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
	if err := truststore.Distribute(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RevokeSSLCert: %s", err)
		http.Error(w,
			fmt.Sprintf("Failed to distribute new truststore to other brokers: %s", err),
			http.StatusInternalServerError)
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
	store, err := store.GetKeystore("keystore")
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
	if err := store.Distribute(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.RemoveSSLCert: %s", err)
		http.Error(w,
			fmt.Sprintf("Failed to distribute keystore to other brokers: %s", err),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func SignCert(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.SignCert: %s", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	var validity string
	if q["validity"] == nil {
		validity = "365"
	} else {
		validity = q["validity"][0]
	}
	signedCert, err := certs.SignCert(string(bytes), validity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.SignCert: %s\n", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Disposition", "attachment; filename=signed_cert.pem")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(signedCert))
}

func UpdateKafkaKeystore(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	defer r.Body.Close()
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.UpdateKafkakeystore: %s\n", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	path := fmt.Sprintf("%s/config/%s", config.KafkaDir, name)
	fmt.Fprintf(os.Stderr, "[INFO] Writing file %s (%d bytes)\n", path, len(bytes))

}
