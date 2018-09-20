package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"injester_test/jobs"
	"injester_test/settings"
	"os"
	"strings"
	"sync"
	"time"
)

/*
Contains classes fo making sure all things works
*/

var (
	//CoreList		[]
	JobList       []jobs.Job
	CachedJobList []jobs.JobJson
	TmpJobList    []jobs.JobJson
	CachedList    []byte
	CachedWorkers int
	Ctx           context.Context
	Cancel        context.CancelFunc
	WG            sync.WaitGroup

	JobCtx    context.Context
	JobCancel context.CancelFunc
	JobWG     sync.WaitGroup
)

func InitScheduler() {
	Ctx, Cancel = context.WithCancel(context.Background())
	WG = sync.WaitGroup{}

	//Setup etcD

	e, err := clientv3.New(clientv3.Config{
		Endpoints:   Settings.Ectd,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		Logger.Error(err)
		os.Exit(1)
	}
	EClient = e
	Logger.Info("connected to etcd - " + strings.Join(Settings.Ectd[:], ","))
	EClient.Delete(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_JOB, clientv3.WithPrefix())
	//EClient.Delete(context.Background(), ETCD_ROOT+"/"+ETCD_URL, clientv3.WithPrefix())

	Logger.Info("DONE")

}

func StopScheduler() {
	StopJobs()
	// tell the goroutines to stop
	Logger.Info("main: telling goroutines to stop")
	Cancel()

	// and wait for them both to reply back
	WG.Wait()
	EClient.Close()
	Logger.Info("main: all goroutines have told us they've finished")
}
func RunCoreJob(job jobs.CoreJob) {
	Logger.Info("Starting Job")
	WG.Add(1)
	job.Init(EClient, Logger, Settings, &Downloads, &Output)
	go job.Run(Ctx, &WG)
}

func StartJobs() {
	Logger.Info("STARTING ALL JOBS")
	Metrics.Update()
	Metrics.NumJobs = len(JobList)
	JobCtx, JobCancel = context.WithCancel(context.Background())
	JobWG = sync.WaitGroup{}
	for _, job := range JobList {
		Logger.Info("Starting Job: ")
		JobWG.Add(1)
		job.Init(EClient, Logger, Settings, &Downloads, &Output)
		go job.Run(JobCtx, &JobWG)
	}
}

func StopJobs() {
	if JobCancel == nil {
		return
	}
	Logger.Info("WAITING STOPPING ALL JOBS")
	JobCancel()
	JobWG.Wait()
	Logger.Info("ALL JOBS HAVE STOPPED")
}

func parseJobList(listStr []byte) {
	StopJobs()
	Logger.Info("Parsing JobList:")
	CachedList = listStr

	TmpJobList = []jobs.JobJson{}
	err := json.Unmarshal(listStr, &TmpJobList)
	if err != nil {
		Logger.Info(string(listStr))
		Logger.Error(err)
		TmpJobList = []jobs.JobJson{}
		return
	}
	SetupJobs()
}

func SetupJobs() {
	if len(TmpJobList) < 1 {
		return
	}
	var divided [][]jobs.JobJson
	numberofWorkers, position, _ := GetNumberOfWorkers()
	chunkSize := (len(TmpJobList)) / (numberofWorkers)
	check := (len(TmpJobList)) % (numberofWorkers)
	if check != 0 {
		chunkSize++
	}
	Logger.Info(fmt.Sprintf("There are %d workers, You are worker %d, ChunkSize: %d, Lenght: %d", numberofWorkers, position+1, chunkSize, len(TmpJobList)))

	for i := 0; i < len(TmpJobList); i += chunkSize {
		end := i + chunkSize
		if end > len(TmpJobList) {
			end = len(TmpJobList)
		}
		divided = append(divided, TmpJobList[i:end])
	}

	StopJobs()
	if position >= len(divided) {
		return
	}

	CachedJobList = divided[position]
	JobList = []jobs.Job{}
	for _, jobjson := range CachedJobList {
		job := jobs.CreateJob(jobjson.Type)
		if job != nil {
			job.New(jobjson)
			JobList = append(JobList, job)
		}
	}
	Metrics.Jobs = CachedJobList
	StartJobs()
}

func GetNumberOfWorkers() (int, int, error) {
	resps, err := EClient.Get(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_SERVICE, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return 0, 0, err
	}
	pos := 0
	for i, v := range resps.Kvs {
		if strings.HasSuffix(string(v.Key), Settings.Hostname) {
			pos = i
		}
	}
	len := len(resps.Kvs)
	if len > 1 {
		len--
	}
	return len, pos, nil
}
