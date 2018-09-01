package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/advancedlogic/GoOse"
	"github.com/coreos/etcd/clientv3"
	"github.com/minio/minio-go"
	"github.com/mmcdole/gofeed"
	"github.com/olivere/elastic"
	"github.com/shirou/gopsutil/load"
	"github.com/sirupsen/logrus"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type SettingStore struct {
	Ectd            []string `yaml:"etcd"`
	NumberOfworkers int      `yaml:"numberWorker"`
	LogLevel        string   `yaml:"LogLevel"`
	LogOutut        string   `yaml:"LogOutput"`
	Hostname        string

	//should store in etcd
	Endpoint        string `yaml:"s3endpoint"`
	AccessKeyID     string `yaml:"s3access"`
	SecretAccessKey string `yaml:"s3secret"`
	UseSSL          bool   `yaml:"s3SSL"`
	BucketName      string `yaml:"s3BucketName"`

	Elastic string `yaml:"elastic"`
	Debug   bool   `yaml:"debug"`
}

type Job struct {
	ID     string `json:id`
	Name   string `json:name`
	Type   string `json:type`
	Params string `json:parms`
	Time   string `json:time`
}

type Metric struct {
	IP            string  `json:ip`
	Hostname      string  `json:Hostname`
	NumJobs       int     `json:numberofjobs`
	JobsProcessed int     `json:jobsproccessed`
	Cpu           float64 `json:cpu`
	Mem           uint64  `json:memory`
	Job           []Job   `json:job`
}

var (
	EClient     *clientv3.Client
	ESclient    *elastic.Client
	Settings    *SettingStore
	downloads   = make(chan string, 100)
	TmpJobList  []Job
	JobList     []Job
	minioClient *minio.Client
	Metrics     = Metric{}

	GoFeedClient = gofeed.NewParser()
	GooseClient = goose.New()
)

func (m *Metric) Update() {
	var met runtime.MemStats
	runtime.ReadMemStats(&met)
	avg, _ := load.Avg()
	m.NumJobs = len(JobList)
	m.Mem = bToMb(met.Alloc)
	m.Cpu = avg.Load1
	m.Job = JobList
}

func (m *Metric) ToString() string {
	e, err := json.Marshal(m)
	if err != nil {
		Logger.Error(err)
		return ""
	}
	return string(e)
}

func (s *SettingStore) SetETCD(etcd []string) {
	if etcd != nil {
		s.Ectd = etcd
	}
}

func (s *SettingStore) SetHostname(hostname string) {
	s.Hostname = hostname
}

func (s *SettingStore) SetETCDString(etcd string) {
	if etcd != "" {
		e := strings.Split(etcd, ",")
		s.Ectd = e
	}
}

func (s *SettingStore) SetNumberOfworkers(workers string) {
	if workers != "" {
		if _, err := strconv.Atoi(workers); err == nil {
			s.NumberOfworkers, _ = strconv.Atoi(workers)
		}
	}
}

func (s *SettingStore) SetLogLevel(level string) {
	if level != "" {
		s.LogLevel = level
	}
}

func (s *SettingStore) setLogOutut(output string) {
	if output != "" {
		s.LogOutut = output
	}
}

func (s SettingStore) print() {
	e := reflect.ValueOf(&s).Elem()
	for i := 0; i < e.NumField(); i++ {
		fieldName := e.Type().Field(i).Name
		fieldValue := e.Field(i)
		Logger.WithFields(logrus.Fields{
			fieldName: fieldValue,
		}).Info("Config:")
	}
}

func (s Job) print() {
	e := reflect.ValueOf(&s).Elem()
	for i := 0; i < e.NumField(); i++ {
		fieldName := e.Type().Field(i).Name
		fieldValue := e.Field(i)
		Logger.WithFields(logrus.Fields{
			fieldName: fieldValue,
		}).Info("Config:")
	}
}

func (j *Job) parseJob() {
	j.Time = time.Now().Format("15:04:05")
	j.ID = Settings.Hostname
	switch jtype := j.Type; jtype {
	case JOB_ARTICLE:
		a := ArticleJob{}
		err := a.FromJob(*j)
		if err != nil {
			Logger.Error(err)
			return
		}
		a.Process()
		a.Clear()
		Metrics.JobsProcessed++
	}
}

func putWithTimeout(key string, value string) error {
	resp, err := EClient.Grant(context.Background(), 10)
	if err != nil {
		log.Fatal(err)
	}
	_, err = EClient.Put(context.Background(), key, value, clientv3.WithLease(resp.ID))
	return err
}

func parseJobList(listStr []byte) {
	Logger.Info("Parsing JobList")
	TmpJobList = []Job{}
	err := json.Unmarshal(listStr, &TmpJobList)
	if err != nil {
		Logger.Info(string(listStr))
		Logger.Error(err)
		TmpJobList = []Job{}
		return
	}
	SetupJobs()
}
func SetupJobs() {
	if len(TmpJobList) < 1 {
		return
	}
	var divided [][]Job
	JobList = []Job{}

	numberofWorkers, position, _ := getNumberOfWorkers()
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
	if position >= len(divided) {
		return
	}

	for _, v := range divided[position] {
		key := ETCD_ROOT + "/" + ETCD_JOB + "/" + v.Name
		value, err := json.Marshal(v)
		if err == nil {
			if putWithTimeout(key, string(value)) == nil {
				JobList = append(JobList, v)
			}
		}
	}
	fmt.Printf("%v\n", JobList)
}

func getNumberOfWorkers() (int, int, error) {

	resps, err := EClient.Get(context.Background(), ETCD_ROOT+"/"+ETCD_SERVICE, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
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
	if(len >1){
		len--
	}
	//ETCD_ROOT+"/"+ETCD_SERVICE is a null key ie directory of V2 or ZK!
	return len, pos, nil
}

func setupWatchers() {

	resps, err := EClient.Get(context.Background(), "e3w_test", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		log.Fatal(err)
	} else {
		for _, v := range resps.Kvs {
			if strings.HasSuffix(string(v.Key), ETCD_JOB_LIST) {
				parseJobList(v.Value)
			}
		}
	}
	go func() {
		Logger.Info("Setting up Watchers")
		watchChan := EClient.Watch(context.Background(), "e3w_test", clientv3.WithPrefix())
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				if (event.Type == clientv3.EventTypePut) || (event.Type == clientv3.EventTypeDelete) {
					//Logger.Info("Updating Store key: "+string(event.Kv.Key))
					if strings.HasSuffix(string(event.Kv.Key), ETCD_JOB_LIST) {
						parseJobList(event.Kv.Value)
					}
					if strings.HasPrefix(string(event.Kv.Key), ETCD_ROOT+"/"+ETCD_SERVICE) {
						SetupJobs()
					}
				}
			}
		}
	}()
}
