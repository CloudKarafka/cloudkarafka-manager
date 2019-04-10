package certs

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

type StoreEntity struct {
	Alias           string `json:"alias"`
	Added           string `json:"added"`
	FingerprintType string `json:"fingerprint_type"`
	Fingerprint     string `json:"fingerprint"`
}

type CertSubject struct {
	CountryCode string
	State       string
	City        string
	Company     string
	Section     string
	CommonName  string
}

func (me CertSubject) String() string {
	var buffer bytes.Buffer
	if me.CountryCode != "" {
		buffer.WriteString("/C=")
		buffer.WriteString(me.CountryCode)
	}
	if me.State != "" {
		buffer.WriteString("/ST=")
		buffer.WriteString(me.State)
	}
	if me.City != "" {
		buffer.WriteString("/L=")
		buffer.WriteString(me.City)
	}
	if me.Company != "" {
		buffer.WriteString("/O=")
		buffer.WriteString(me.Company)
	}
	if me.Section != "" {
		buffer.WriteString("/OU=")
		buffer.WriteString(me.Section)
	}
	if me.CommonName != "" {
		buffer.WriteString("/CN=")
		buffer.WriteString(me.CommonName)
	}
	return buffer.String()
}

type PublicPrivateKeyPair struct {
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
}

var EmptyKeyPair = PublicPrivateKeyPair{}

func (me PublicPrivateKeyPair) genFile(content string) (*os.File, error) {
	tmpfile, err := ioutil.TempFile("", "key_")
	if err != nil {
		return nil, err
	}
	if _, err := tmpfile.Write([]byte(content)); err != nil {
		return nil, err
	}
	if err := tmpfile.Close(); err != nil {
		return nil, err
	}
	return tmpfile, nil
}
func (me PublicPrivateKeyPair) PublicKeyFile() (*os.File, error) {
	return me.genFile(me.PublicKey)
}
func (me PublicPrivateKeyPair) PrivateKeyFile() (*os.File, error) {
	return me.genFile(me.PrivateKey)
}

type JKS struct {
	Path     string
	Password string
	Type     string
}

var ignoreParams = []string{"-deststorepass", "-srcstorepass", "-destkeypass", "-password"}

func logCommand(desc string, cmd *exec.Cmd) {
	skipNext := false
	var buffer bytes.Buffer
	buffer.WriteString(cmd.Path)
	for _, v := range cmd.Args[1:] {
		if skipNext {
			buffer.WriteString(" *******")
		} else {
			buffer.WriteString(" " + v)
		}
		skipNext = false
		for _, skip := range ignoreParams {
			if v == skip {
				skipNext = true
			}
		}
	}
	fmt.Fprintf(os.Stderr, "[INFO] %s: %s\n", desc, buffer.String())
}

func (me JKS) List() ([]StoreEntity, error) {
	cmd := exec.Command("keytool",
		"-keystore", me.Path,
		"-storepass", me.Password,
		"-list")
	logCommand("List entries in keystore", cmd)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("Failed to execute command: %s\n%s", err, out)
	}
	rows := strings.Split(string(out), "\n")
	res := make([]StoreEntity, 0)
	fingerprintExp := regexp.MustCompile(`^Certificate fingerprint \((.*?)\): (.*?)$`)
	for i := 5; i < len(rows); i++ {
		if rows[i] != "" {
			p := strings.Split(rows[i], ",")
			fingerprintMatch := fingerprintExp.FindStringSubmatch(rows[i+1])
			res = append(res, StoreEntity{
				p[0],
				strings.TrimSpace(p[1] + "," + p[2]),
				fingerprintMatch[1],
				fingerprintMatch[2]})
			i += 1
		}
	}
	return res, nil
}
func ValidateCert(cert string) (bool, error) {
	tmpfile, err := ioutil.TempFile("", "cert_")
	if err != nil {
		return false, err
	}
	defer os.Remove(tmpfile.Name()) // clean up
	if _, err := tmpfile.WriteString(cert); err != nil {
		return false, err
	}
	if err := tmpfile.Close(); err != nil {
		return false, err
	}
	cmd := exec.Command("openssl", "x509", "-in", tmpfile.Name())
	_, err = cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] Validate certificate failed with: %s", err)
		return false, nil
	}
	return true, nil
}

func GenerateCert(privateKey string, subject CertSubject, validity string) (PublicPrivateKeyPair, error) {
	certnameFile, err := ioutil.TempFile("", "cert_")
	if err != nil {
		return EmptyKeyPair, err
	}
	defer os.Remove(certnameFile.Name())

	if privateKey == "" {
		// Need to generate new private key
		keynameFile, err := ioutil.TempFile("", "cert_")
		if err != nil {
			return EmptyKeyPair, err
		}
		defer os.Remove(keynameFile.Name())
		cmd := exec.Command("openssl", "req",
			"-x509", "-sha256", "-nodes",
			"-newkey", "rsa:2048",
			"-days", validity,
			"-subj", subject.String(),
			"-keyout", keynameFile.Name(),
			"-out", certnameFile.Name())
		logCommand("Generate private key and cert", cmd)
		_, err = cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Command failed: %s", err)
			return EmptyKeyPair, errors.New("Command to generate certificate failed")

		}
		privateKeyBytes, err := ioutil.ReadFile(keynameFile.Name())
		privateKey = string(privateKeyBytes)
		if err != nil {
			return EmptyKeyPair, err
		}
	} else {
		tmpfile, err := ioutil.TempFile("", "key_")
		if err != nil {
			return EmptyKeyPair, err
		}
		if _, err := tmpfile.WriteString(privateKey); err != nil {
			return EmptyKeyPair, err
		}
		if err := tmpfile.Close(); err != nil {
			return EmptyKeyPair, err
		}
		defer os.Remove(tmpfile.Name())
		cmd := exec.Command("openssl", "req",
			"-x509", "-sha256", "-new",
			"-key", tmpfile.Name(),
			"-days", validity,
			"-subj", subject.String(),
			"-out", certnameFile.Name())
		logCommand("Generate new cert from private key", cmd)
		out, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Command failed: %s\n%s", err, out)
			return EmptyKeyPair, errors.New("Command to generate certificate failed")
		}
	}
	certContent, err := ioutil.ReadFile(certnameFile.Name())
	if err != nil {
		return EmptyKeyPair, err
	}
	return PublicPrivateKeyPair{string(certContent), privateKey}, nil
}

func (me JKS) ImportCert(cert, alias string) error {
	tmpfile, err := ioutil.TempFile("", "cert_")
	if err != nil {
		return err
	}
	// defer os.Remove(tmpfile.Name()) // clean up
	if _, err := tmpfile.WriteString(cert); err != nil {
		return err
	}
	if err := tmpfile.Close(); err != nil {
		return err
	}
	cmd := exec.Command("keytool", "-import",
		"-alias", alias,
		"-file", tmpfile.Name(),
		"-keystore", me.Path,
		"-storepass", me.Password,
		"-noprompt")
	logCommand("Import cert to keystore", cmd)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("Failed to execute command: %s\n Out=%s", err, out)
	}
	return nil
}

func (me JKS) ImportPrivateKey(pair PublicPrivateKeyPair, alias string) error {
	tmpfile, err := ioutil.TempFile("", "keystore_")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not create temporary keystore: %s\n", err)
		return errors.New("Could not create temporary file, action failed")
	}
	defer os.Remove(tmpfile.Name())
	pubKeyFile, err := pair.PublicKeyFile()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not create temporary file: %s", err)
		return errors.New("Could not create temporary file, action failed")
	}
	privKeyFile, err := pair.PrivateKeyFile()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not create temporary file: %s\n", err)
		return errors.New("Could not create temporary file, action failed")
	}
	defer os.Remove(pubKeyFile.Name())
	defer os.Remove(privKeyFile.Name())
	cmd := exec.Command("openssl", "pkcs12", "-export",
		"-in", pubKeyFile.Name(),
		"-inkey", privKeyFile.Name(),
		"-out", tmpfile.Name(),
		"-name", alias,
		"-password", "pass:supersecret") // Temporary keystore, no need for a good password
	logCommand("Generate new keystore with cert and private key", cmd)
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not generate keystore: %s\n%s", err, out)
		return errors.New("Could not generate keystore, action failed")
	}
	cmd2 := exec.Command("keytool", "-importkeystore",
		"-deststorepass", me.Password,
		"-destkeypass", me.Password,
		"-destkeystore", me.Path,
		"-srckeystore", tmpfile.Name(),
		"-srcstoretype", "PKCS12",
		"-srcstorepass", "supersecret",
		"-alias", alias,
		"-noprompt")
	logCommand("Import all items in one keystore to another", cmd2)
	out, err = cmd2.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not import private key to keystore: %s\n%s", err, out)
		return errors.New("Could not import key to keystore, action failed")
	}
	return nil
}

func (me JKS) RenewCert(alias string, subject CertSubject, validity string) (PublicPrivateKeyPair, error) {
	tmpfile, err := ioutil.TempDir("", "keystore_")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not create temporary keystore: %s\n", err)
		return EmptyKeyPair, errors.New("Could not create temporary file, action failed")
	}
	defer os.RemoveAll(tmpfile)
	keystoreCommand := exec.Command("keytool", "-importkeystore",
		"-srcstorepass", me.Password,
		"-srckeystore", me.Path,
		"-srcalias", alias,
		"-destkeystore", filepath.Join(tmpfile, "store"),
		"-deststoretype", "PKCS12",
		"-deststorepass", "supersecret",
		"-destkeypass", "supersecret",
		"-noprompt")
	logCommand("Merge one keystore into another", keystoreCommand)
	out, err := keystoreCommand.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not export private key from keystore: %s\n %s", err, out)
		return EmptyKeyPair, errors.New("Could not get private key from keystore")
	}
	tmpKeyfile, err := ioutil.TempFile("", "key_")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not create temporary key file: %s\n", err)
		return EmptyKeyPair, errors.New("Could not create temporary file, action failed")
	}
	defer os.Remove(tmpKeyfile.Name())
	convertCommand := exec.Command("openssl", "pkcs12",
		"-in", filepath.Join(tmpfile, "store"),
		"-nodes", "-nocerts",
		"-out", tmpKeyfile.Name(),
		"-password", "pass:supersecret")
	logCommand("Export private key from keystore", convertCommand)
	out, err = convertCommand.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't convert keystore to private key: %s\n %s", err, out)
		return EmptyKeyPair, errors.New("Could not get private key from keystore")
	}
	bytes, err := ioutil.ReadAll(tmpKeyfile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Couldn't read tmp key file: %s\n", err)
		return EmptyKeyPair, errors.New("Could not read from temporary file, action failed")
	}
	return GenerateCert(string(bytes), subject, validity)

}

func (me JKS) RemoveEntry(alias string) error {
	cmd := exec.Command("keytool", "-keystore", me.Path,
		"-storepass", me.Password,
		"-alias", alias,
		"-noprompt", "-delete")
	logCommand("Delete entry from keystore", cmd)
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to execute command %s\n%s", err, out)
		return errors.New("Command to generate certificate failed")
	}
	return nil
}

func (me JKS) DeleteStore() error {
	return os.Remove(me.Path)
}
