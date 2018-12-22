package scheduler

import (
	"bufio"
	"fmt"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"injester_test/leaderElection"
	"injester_test/pipeline"
	"injester_test/settings"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var Health = make(map[string]*Metrics)
var containerMemory = uint64(0)

var HEALTHY = "healthy"
var UNHEALTHY = "unhealthy"

func init() {
	if _, err := os.Stat("/.container"); !os.IsNotExist(err) {
		file, err := os.Open("/sys/fs/cgroup/memory/memory.stat")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			data := strings.Split(scanner.Text(), " ")
			if data[0] == "hierarchical_memory_limit" {
				memorylimit, _ := strconv.Atoi(data[1])
				memorylimit = memorylimit / (1024 * 1024)
				containerMemory = uint64(memorylimit)
				return
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}

}

func CheckHealth() bool {
	current := len(Health)
	for k, v := range Health {
		t, _ := time.Parse(time.RFC3339, v.LastUpdated)
		d := time.Since(t)
		if d.Seconds() > 10 {
			delete(Health, k)
		}
	}
	return current == len(Health)
}

func GetHealth() map[string]*Metrics {

	fmt.Println(Health)

	return Health
}

type Metrics struct {
	Worker          string                    `json:"worker"`
	Ip              string                    `json:"ip"`
	NumberPipelines int                       `json:"number_pipelines"`
	LastUpdated     string                    `json:"last_updated"`
	Status          string                    `json:"status"`
	Stats           []pipeline.PipelineStatus `json:"stats"`
	MemoryUsed      uint64                    `json:"memory_used"`
	MemoryTotal     uint64                    `json:"memory_total"`
	MemoryPercent   float64                   `json:"memory_percent"`

	Load   float64 `json:"load"`
	Uptime uint64  `json:"uptime"`
}

func GetMetrics() *Metrics {
	m := Metrics{}
	v, _ := mem.VirtualMemory()
	c, _ := load.Avg()

	var met runtime.MemStats
	runtime.ReadMemStats(&met)

	m.Worker = settings.Settings.Hostname
	m.Ip, _ = leaderElection.GetExternalIP()
	m.LastUpdated = time.Now().Format(time.RFC3339)
	m.NumberPipelines = len(Scheduler.AllStatusProcess())
	m.Stats = Scheduler.AllStatusProcess()
	m.Status = HEALTHY

	m.MemoryUsed = bytesToMegaBytes(met.Alloc)

	if containerMemory == uint64(0) {
		m.MemoryPercent = v.UsedPercent
		m.MemoryTotal = bytesToMegaBytes(v.Total)
	} else {
		calc := (1 - (float64(containerMemory-m.MemoryUsed) / float64(containerMemory))) * 100
		m.MemoryPercent = float64(calc)
		m.MemoryTotal = (containerMemory)
	}

	m.Load = c.Load5
	m.Uptime, _ = host.Uptime()

	if m.MemoryPercent > 50 {
		m.Status = UNHEALTHY
	}

	return &m
}

func bytesToMegaBytes(byest uint64) uint64 {
	return byest / (1024 * 1024)
}
