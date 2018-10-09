package zookeeper

import (
	"fmt"
	"testing"
)

func verifySpread(t *testing.T, expected, actual [][]int) {
	if len(expected) != len(actual) {
		t.Error("Not the same length on partitions")
	}
	for k, r := range expected {
		for i := 0; i < len(r); i++ {
			if r[i] != actual[k][i] {
				fmt.Println(expected, actual)
				t.Fail()
				return
			}
		}
	}
}

func TestPermutation(t *testing.T) {
	expected := [][]int{
		[]int{0, 1, 2},
		[]int{1, 0, 2},
		[]int{2, 1, 0},
		[]int{0, 2, 1},
		[]int{1, 2, 0},
		[]int{2, 0, 1},
	}
	brokers := []int{0, 1, 2}
	actual := replicaSpreads(brokers, len(brokers))
	verifySpread(t, expected, actual)
}

func TestAddPartitions(t *testing.T) {
	pl := partitionList{
		[]int{0},
		[]int{0},
	}
	expected := [][]int{
		[]int{0},
		[]int{0},
		[]int{0},
		[]int{0},
	}
	brokers := []int{0}
	res := updatePartitions(brokers, pl, 1, 4)
	verifySpread(t, expected, res)
}
func TestAddPartitions2(t *testing.T) {
	pl := partitionList{
		[]int{0},
		[]int{0},
	}
	expected := [][]int{
		[]int{0, 1, 2},
		[]int{1, 0, 2},
		[]int{2, 1, 0},
		[]int{0, 2, 1},
	}
	brokers := []int{0, 1, 2}
	res := updatePartitions(brokers, pl, 3, 4)
	verifySpread(t, expected, res)
}

// func updatePartitions(brokers []int, pl partitionList, replicationFactor, partitionCount int) partitionList {
func TestAddBrokers(t *testing.T) {
	pl := partitionList{
		[]int{0},
		[]int{0},
		[]int{0},
		[]int{0},
	}
	expected := [][]int{
		[]int{0, 1, 2},
		[]int{1, 0, 2},
		[]int{2, 1, 0},
		[]int{0, 2, 1},
	}
	brokers := []int{0, 1, 2}
	res := updatePartitions(brokers, pl, 3, 4)
	verifySpread(t, expected, res)
}

func TestRemoveBrokers(t *testing.T) {
	pl := partitionList{
		[]int{0, 1, 2, 3, 4},
		[]int{1, 0, 2, 3, 4},
		[]int{2, 1, 0, 3, 4},
		[]int{3, 1, 2, 0, 4},
		[]int{4, 1, 2, 3, 0},
	}
	expected := [][]int{
		[]int{0, 1, 2},
		[]int{1, 0, 2},
		[]int{2, 1, 0},
		[]int{0, 2, 1},
		[]int{1, 2, 0},
	}
	brokers := []int{0, 1, 2}
	res := updatePartitions(brokers, pl, 3, 5)
	verifySpread(t, expected, res)
}

func TestDecreaseReplicationOnPartitions(t *testing.T) {
	pl := [][]int{
		[]int{0, 1, 2},
		[]int{1, 0, 2},
		[]int{2, 1, 0},
		[]int{0, 2, 1},
	}
	expected := [][]int{
		[]int{0, 1},
		[]int{1, 0},
		[]int{2, 1},
		[]int{0, 2},
	}
	brokers := []int{0, 1, 2}
	res := updatePartitions(brokers, pl, 2, 4)
	verifySpread(t, expected, res)
}

func TestSpreadLeader(t *testing.T) {
	pl := partitionList{
		[]int{0},
		[]int{0},
		[]int{0},
		[]int{0},
		[]int{0},
	}
	expected := [][]int{
		[]int{0},
		[]int{1},
		[]int{2},
		[]int{0},
		[]int{1},
	}
	brokers := []int{0, 1, 2}
	res := spreadPartitions(pl, brokers, 1)
	verifySpread(t, expected, res)
}
