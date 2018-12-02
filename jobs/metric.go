package jobs

import (
	"encoding/json"
	"fmt"
	"github.com/shirou/gopsutil/load"
	"runtime"
	"time"
)

type Metric struct {
	IP                 string `json:"ip"`
	Hostname           string `json:"Hostname"`
	NumJobs            int    `json:"numberofjobs"`
	JobsProcessed      int    `json:"jobsproccessed"`
	TotalJobsProcessed int
	Cpu                float64   `json:"cpu"`
	Mem                uint64    `json:"memory"`
	Jobs               []JobJson `json:"jobs"`
	Time               time.Time `json:"time"`
}

func (m Metric) New(hostname string) *Metric {
	m.Hostname = hostname
	return &m
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func (m *Metric) Update() {
	var met runtime.MemStats
	runtime.ReadMemStats(&met)
	avg, _ := load.Avg()
	m.Mem = bToMb(met.Alloc)
	m.Cpu = avg.Load1
	m.Time = time.Now()
}

func (m *Metric) ToString() string {
	e, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return string(e)
}
