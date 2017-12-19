package store

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

type t struct {
	Name string
}

func tairStore() {
	Store = make([]Data, 0)
}

func seedStore(elems int) {
	for i := 0; i < elems; i++ {
		var st storageType
		if i%2 == 0 {
			st = JMX
		} else {
			st = Group
		}
		Put(map[string]string{
			"Name": fmt.Sprintf("test%v", i),
			"Type": st.String(),
		}, 10)
	}
}

func TestMain(m *testing.M) {
	seedStore(10000)
	os.Exit(m.Run())
}

func TestSelectAllOfType(test *testing.T) {
	selected := Store.Select(func(data Data) bool {
		return data.Id["Type"] == JMX.String()
	})
	if len(selected) != 5000 {
		test.Error("Expected 5000 stored element got", len(selected))
	}
}

func TestFind(test *testing.T) {
	data, err := Store.Find(func(d Data) bool {
		return d.Id["Name"] == "test0"
	})
	if err != nil {
		test.Error("Expected test0 found nothing")
	}
	if data.Id["Name"] != "test0" {
		test.Error("Expected test0 got", data.Id["Name"])
	}
}

func benchmarkStore(elems int, b *testing.B) {
	seedStore(elems)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Store.Select(func(d Data) bool {
			return strings.HasPrefix(d.Id["Name"], fmt.Sprintf("test%v", n))
		})
	}
	b.StopTimer()
	tairStore()
}

func BenchmarkStore1(b *testing.B)     { benchmarkStore(1, b) }
func BenchmarkStore10(b *testing.B)    { benchmarkStore(10, b) }
func BenchmarkStore100(b *testing.B)   { benchmarkStore(100, b) }
func BenchmarkStore1000(b *testing.B)  { benchmarkStore(1000, b) }
func BenchmarkStore10000(b *testing.B) { benchmarkStore(10000, b) }
