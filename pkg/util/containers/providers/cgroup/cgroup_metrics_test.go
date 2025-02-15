// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build linux
// +build linux

package cgroup

import (
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCPU(t *testing.T) {
	tempFolder := newTempFolder(t)

	cpuacctStats := dummyCgroupStat{
		"user":   64140,
		"system": 18327,
	}
	tempFolder.add("cpuacct/cpuacct.stat", cpuacctStats.String())
	tempFolder.add("cpuacct/cpuacct.usage", "915266418275")
	tempFolder.add("cpu/cpu.shares", "1024")

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "cpuacct", "cpu")

	timeStat, err := cgroup.CPU()
	assert.Nil(t, err)
	assert.Equal(t, timeStat.User, float64(64140))
	assert.Equal(t, timeStat.System, float64(18327))
	assert.Equal(t, timeStat.Shares, float64(1024))
	assert.InDelta(t, timeStat.UsageTotal, 91526.6418275, 0.0000001)
}

func TestCPULimit(t *testing.T) {
	tempFolder := newTempFolder(t)

	// CFS period and quota
	tempFolder.add("cpu/cpu.cfs_period_us", "100000")
	tempFolder.add("cpu/cpu.cfs_quota_us", "600000")

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "cpu")
	cpuLimit, err := cgroup.CPULimit()

	assert.Nil(t, err)
	assert.Equal(t, cpuLimit, float64(600))

	// CPU set
	tempFolder.add("cpuset/cpuset.cpus", "0-4")

	cgroup = newDummyContainerCgroup(tempFolder.RootPath, "cpu", "cpuset")
	cpuLimit, err = cgroup.CPULimit()

	assert.Nil(t, err)
	assert.Equal(t, cpuLimit, float64(500))
}

func TestCPUNrThrottled(t *testing.T) {
	tempFolder := newTempFolder(t)

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "cpu")

	// No file
	throttled, throttledTime, err := cgroup.CPUPeriods()
	assert.Nil(t, err)
	assert.Equal(t, throttled, uint64(0))
	assert.Equal(t, throttledTime, float64(0))

	// Invalid file
	tempFolder.add("cpu/cpu.stat", "200")
	_, _, err = cgroup.CPUPeriods()
	assert.Nil(t, err)

	// Valid file
	cpuStats := dummyCgroupStat{
		"nr_periods":     20,
		"nr_throttled":   10,
		"throttled_time": 18327,
	}
	tempFolder.add("cpu/cpu.stat", cpuStats.String())
	throttled, throttledTime, err = cgroup.CPUPeriods()
	assert.Nil(t, err)
	assert.Equal(t, throttled, uint64(10))
	assert.Equal(t, throttledTime, float64(18327)/NanoToUserHZDivisor)
}

func TestMemLimit(t *testing.T) {
	tempFolder := newTempFolder(t)

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "memory")

	// No file
	value, err := cgroup.MemLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(0))

	// Invalid file
	tempFolder.add("memory/memory.limit_in_bytes", "ab")
	value, err = cgroup.MemLimit()
	assert.NotNil(t, err)
	assert.IsType(t, err, &strconv.NumError{})
	assert.Equal(t, value, uint64(0))

	// Overflow value
	tempFolder.add("memory/memory.limit_in_bytes", strconv.Itoa(int(math.Pow(2, 61))))
	value, err = cgroup.MemLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(0))

	// Valid value
	tempFolder.add("memory/memory.limit_in_bytes", "1234")
	value, err = cgroup.MemLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(1234))
}

func TestSoftMemLimit(t *testing.T) {
	tempFolder := newTempFolder(t)

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "memory")

	// No file
	value, err := cgroup.SoftMemLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(0))

	// Invalid file
	tempFolder.add("memory/memory.soft_limit_in_bytes", "ab")
	value, err = cgroup.SoftMemLimit()
	assert.NotNil(t, err)
	assert.IsType(t, err, &strconv.NumError{})
	assert.Equal(t, value, uint64(0))

	// Overflow value
	tempFolder.add("memory/memory.soft_limit_in_bytes", strconv.Itoa(int(math.Pow(2, 61))))
	value, err = cgroup.SoftMemLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(0))

	// Valid value
	tempFolder.add("memory/memory.soft_limit_in_bytes", "1234")
	value, err = cgroup.SoftMemLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(1234))
}

func TestParseSingleStat(t *testing.T) {
	tempFolder := newTempFolder(t)

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "cpu")

	// No file
	_, err := cgroup.ParseSingleStat("cpu", "notfound")
	assert.NotNil(t, err)
	assert.True(t, os.IsNotExist(err))

	// Several lines
	tempFolder.add("cpu/cpu.test", "1234\nbla")
	_, err = cgroup.ParseSingleStat("cpu", "cpu.test")
	assert.NotNil(t, err)
	t.Log(err)
	assert.Contains(t, err.Error(), "wrong file format")

	// Not int
	tempFolder.add("cpu/cpu.test", "1234bla")
	_, err = cgroup.ParseSingleStat("cpu", "cpu.test")
	assert.NotNil(t, err)
	t.Log(err)
	assert.Equal(t, err.Error(), "strconv.ParseUint: parsing \"1234bla\": invalid syntax")

	// Valid file
	tempFolder.add("cpu/cpu.test", "1234")
	value, err := cgroup.ParseSingleStat("cpu", "cpu.test")
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(1234))
}

func TestThreadLimit(t *testing.T) {
	tempFolder := newTempFolder(t)

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "pids")

	// No file
	value, err := cgroup.ThreadLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(0))

	// Invalid file
	tempFolder.add("pids/pids.max", "ab")
	value, err = cgroup.ThreadLimit()
	assert.NotNil(t, err)
	assert.IsType(t, err, &strconv.NumError{})
	assert.Equal(t, value, uint64(0))

	// No limit
	tempFolder.add("pids/pids.max", "max")
	value, err = cgroup.ThreadLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(0))

	// Valid value
	tempFolder.add("pids/pids.max", "1234")
	value, err = cgroup.ThreadLimit()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(1234))
}

func TestThreadCount(t *testing.T) {
	tempFolder := newTempFolder(t)

	cgroup := newDummyContainerCgroup(tempFolder.RootPath, "pids")

	// No file
	value, err := cgroup.ThreadCount()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(0))

	// Invalid file
	tempFolder.add("pids/pids.current", "ab")
	value, err = cgroup.ThreadCount()
	assert.NotNil(t, err)
	assert.IsType(t, err, &strconv.NumError{})
	assert.Equal(t, value, uint64(0))

	// Valid value
	tempFolder.add("pids/pids.current", "123")
	value, err = cgroup.ThreadCount()
	assert.Nil(t, err)
	assert.Equal(t, value, uint64(123))
}
