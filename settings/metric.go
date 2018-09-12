package settings

import (
	"encoding/json"
	"fmt"
	"github.com/shirou/gopsutil/load"
	"runtime"
)

type Metric struct {
	IP            string  `json:ip`
	Hostname      string  `json:Hostname`
	NumJobs       int     `json:numberofjobs`
	JobsProcessed int     `json:jobsproccessed`
	Cpu           float64 `json:cpu`
	Mem           uint64  `json:memory`
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func (m *Metric) Update(store *SettingStore) {
	var met runtime.MemStats
	runtime.ReadMemStats(&met)
	avg, _ := load.Avg()
	m.Hostname = store.Hostname
	//m.NumJobs = len(JobList)
	m.Mem = bToMb(met.Alloc)
	m.Cpu = avg.Load1
}

func (m *Metric) ToString() string {
	e, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return string(e)
}
