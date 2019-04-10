package certs

import (
	"strings"
	"testing"
)

func TestGenerate(t *testing.T) {
	subject := CertSubject{"SE", "Stockholm", "Stockholm", "84codes", "Dev", "test"}
	pair, err := GenerateCert("", subject, "1")
	if err != nil {
		t.Fail()
	}
	if !strings.Contains(pair.PublicKey, "BEGIN CERTIFICATE") {
		t.Error("No public cert")
	}
}

func TestValidate(t *testing.T) {
	subject := CertSubject{"SE", "Stockholm", "Stockholm", "84codes", "Dev", "test"}
	pair, err := GenerateCert("", subject, "1")
	if err != nil {
		t.Errorf("Error generate cert: %s", err)
	}
	valid, err := ValidateCert(pair.PublicKey)
	if err != nil {
		t.Errorf("Error validate cert: %s", err)
	}
	if !valid {
		t.Fail()
	}
}

func TestImportCert(t *testing.T) {
	jks := &JKS{"truststore_test.jks", "testpasswd", "pkcs12"}
	defer jks.DeleteStore()
	subject := CertSubject{"SE", "Stockholm", "Stockholm", "84codes", "Dev", "test"}
	pair, err := GenerateCert("", subject, "1")
	if err != nil {
		t.Error(err.Error())
	}
	if err = jks.ImportCert(pair.PublicKey, "testalias"); err != nil {
		t.Error(err.Error())
	}
	certs, err := jks.List()
	if err != nil {
		t.Error(err.Error())
	}
	if len(certs) != 1 {
		t.Error("No certs in truststore")
	}
}

func TestRemoveCert(t *testing.T) {
	jks := &JKS{"truststore_test.jks", "testpasswd", "pkcs12"}
	defer jks.DeleteStore()
	subject := CertSubject{"SE", "Stockholm", "Stockholm", "84codes", "Dev", "test"}
	pair, err := GenerateCert("", subject, "1")
	if err != nil {
		t.Error(err.Error())
	}
	if err = jks.ImportCert(pair.PublicKey, "testalias"); err != nil {
		t.Error(err.Error())
	}
	certs, err := jks.List()
	if err != nil {
		t.Error(err.Error())
	}
	if len(certs) != 1 {
		t.Error("No certs in truststore")
	}

	if err := jks.RemoveEntry("testalias"); err != nil {
		t.Error(err.Error())
	}
	certs, err = jks.List()
	if err != nil {
		t.Error(err.Error())
	}
	if len(certs) != 0 {
		t.Error("Still got 1 cert in trust store")
	}
}

func TestImportPrivateKey(t *testing.T) {
	keystore := &JKS{"keystore_test.jks", "testpasswd", "pkcs12"}
	defer keystore.DeleteStore()
	subject := CertSubject{"SE", "Stockholm", "Stockholm", "84codes", "Dev", "test"}
	pair, err := GenerateCert("", subject, "1")
	if err != nil {
		t.Error(err.Error())
	}
	if err = keystore.ImportPrivateKey(pair, "test_key_alias"); err != nil {
		t.Error(err.Error())
	}
	entries, _ := keystore.List()
	if entries[0].Alias != "test_key_alias" {
		t.Fail()
	}
}

func TestImportMultiplePrivateKey(t *testing.T) {
	keystore := &JKS{"keystore_test.jks", "testpasswd", "pkcs12"}
	defer keystore.DeleteStore()
	subject := CertSubject{"SE", "Stockholm", "Stockholm", "84codes", "Dev", "test1"}
	pair, err := GenerateCert("", subject, "1")
	if err != nil {
		t.Error("Failed to generate Root cert", err.Error())
		return
	}
	if err = keystore.ImportPrivateKey(pair, "test1"); err != nil {
		t.Error("Failed to import Root cert", err.Error())
		return
	}
	pair, err = GenerateCert("", CertSubject{"", "", "", "", "", "test2"}, "1")
	if err != nil {
		t.Error("Failed to genrate client cert", err.Error())
		return
	}
	if err = keystore.ImportPrivateKey(pair, "test2"); err != nil {
		t.Error("Failed to import client cert", err.Error())
		return
	}
	entries, _ := keystore.List()
	if len(entries) != 2 {
		t.Logf("Expected 2 entries got %v", len(entries))
		t.Fail()
		return
	}
	if entries[0].Alias != "test1" {
		t.Logf("Expected first alias to match test1 got %v", entries[0].Alias)
		t.Fail()
		return
	}
	if entries[1].Alias != "test2" {
		t.Logf("Expected second alias to match test2 got %s", entries[1].Alias)
		t.Fail()
		return
	}
	keystore.RemoveEntry("test1")
	entries, _ = keystore.List()
	if len(entries) != 1 {
		t.Logf("Expected 1 entry got %v", len(entries))
		t.Fail()
		return
	}
	if entries[0].Alias != "test2" {
		t.Logf("Expected first alias to match test2 got %s", entries[0].Alias)
		t.Fail()
		return
	}
}

func TestRenewCert(t *testing.T) {
	keystore := &JKS{"keystore_test.jks", "testpasswd", "pkcs12"}
	defer keystore.DeleteStore()
	subject := CertSubject{"SE", "Stockholm", "Stockholm", "84codes", "Dev", "test"}
	pair, err := GenerateCert("", subject, "1")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err = keystore.ImportPrivateKey(pair, "test"); err != nil {
		t.Error(err.Error())
		return
	}
	pair, err = keystore.RenewCert("test", CertSubject{"", "", "", "", "", "test"}, "1")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !strings.Contains(pair.PublicKey, "BEGIN CERTIFICATE") {
		t.Logf("Expected public key to start with 'BEGIN CERTIFICATE'\n%s", pair.PublicKey)
		t.Fail()
	}
}
