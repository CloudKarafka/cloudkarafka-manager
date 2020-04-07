package config

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func writeTmpfile(content, prefix string) (*os.File, error) {
	tmpfile, err := ioutil.TempFile("", prefix)
	if err != nil {
		return tmpfile, fmt.Errorf("Could not create temporary file: %s", err)
	}
	if content != "" {
		if _, err := tmpfile.WriteString(content); err != nil {
			return tmpfile, fmt.Errorf("Could not create temporary file: %s", err)
		}
		if err := tmpfile.Close(); err != nil {
			return tmpfile, fmt.Errorf("Could not create temporary file: %s", err)
		}
	}
	return tmpfile, nil
}

func TestParseKafkaConfig(t *testing.T) {
	content := `a = b
version=2.1.0
# some commented line

#some.old.key=with.value
some.longer.key=some.value`
	reader := strings.NewReader(content)
	conf := ParseKafkaConfig(reader)
	if len(conf.Items) != 5 {
		t.Error("Config has 5 lines")
	}
	if len(conf.Values()) != 3 {
		t.Error("Config has 3 valid settings")
	}

	// Add new value to config
	conf.Update("new.key", "new.value")
	if len(conf.Values()) != 4 {
		t.Error("Config has 4 valid settings")
	}

	// Update exiting value
	// Should comment out the old value and set date to new
	conf.Update("new.key", "new.value2")
	if len(conf.Items) != 7 {
		t.Error("Config has 6 valid settings")
	}
	values := conf.Values()
	if len(values) != 4 {
		t.Errorf("Config should have 4 valid settings, has %d", len(values))
	}
	var b bytes.Buffer
	conf.Write(&b)
	writterLines := strings.Split(b.String(), "\n")
	expectedLines := []string{
		"a=b",
		"version=2.1.0",
		"# some commented line",
		"# some.old.key=with.value",
		"some.longer.key=some.value",
		"", // Empty strings will skip comparison
		"# new.key=new.value",
		"",
		"new.key=new.value2",
		"",
	}
	if len(writterLines) != len(expectedLines) {
		t.Error("Written config isn't correct length")
		return
	}
	for i, l := range writterLines {
		if l != expectedLines[i] && expectedLines[i] != "" {
			t.Errorf("Line %d is wrong: %s != %s", i+1, l, expectedLines[i])
		}
	}
}
