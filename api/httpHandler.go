package api

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
	"injester_test/jobs"
	"injester_test/pipeline"
	"injester_test/settings"
	"log"
	"net/http"
)

type PageVariables struct {
	Jobs      []jobs.JobJson
	Metric    *jobs.Metric
	Time      string
	Service   []jobs.Metric
	JobParmas []pipeline.PipeLineParams
}

var HomePageVars = PageVariables{JobParmas: pipeline.PipelineParamsList}

var (
	EClient    *clientv3.Client
	ESclient   *elastic.Client
	Settings   *settings.SettingStore
	Logger     *logrus.Logger
	Metrics    *jobs.Metric
	Downloads  = make(chan string, 100)
	Output     = make(chan jobs.Model, 100)
	TmpJobList []pipeline.PipeLineJson
)

func getData(w http.ResponseWriter, r *http.Request) {
	HomePageVars.Metric = Metrics
	HomePageVars.Jobs = []jobs.JobJson{}

	resps, err := EClient.Get(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_SERVICE, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		log.Fatal(err)
	} else {
		for _, v := range resps.Kvs {
			m := jobs.Metric{}
			err := json.Unmarshal((v.Value), &m)
			if err == nil {
				HomePageVars.Service = append(HomePageVars.Service, m)
				for i := range HomePageVars.Jobs {
					job := &HomePageVars.Jobs[i]
					for _, v := range m.Jobs {
						if v.Name == job.Name {
							job.ID = m.Hostname
							job.Time = string(m.Time.Format("2006-01-02 15:04:05"))
						}
					}
				}
			}
		}
	}
	js, err := json.Marshal(HomePageVars)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func postData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	decoder := json.NewDecoder(r.Body)
	var job pipeline.PipeLineJson
	err := decoder.Decode(&job)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	var jobList = TmpJobList
	jobList = append(jobList, job)
	json, err := json.Marshal(jobList)
	_, err = EClient.Put(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_JOB_LIST, string(json))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	} else {
		w.Write([]byte("success"))
	}

}

func deleteJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	decoder := json.NewDecoder(r.Body)
	var job jobs.JobJson
	err := decoder.Decode(&job)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	jobList := TmpJobList[:0]
	for _, j := range TmpJobList {
		if j.Name != job.Name {
			jobList = append(jobList, j)
		}
	}
	json, err := json.Marshal(jobList)
	_, err = EClient.Put(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_JOB_LIST, string(json))

	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	} else {
		w.Write([]byte("success"))
	}
}

func GetSettings(w http.ResponseWriter, r *http.Request) {
	js, err := json.Marshal(pipeline.PipelineParamsList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func HomePage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		getData(w, r)
	case http.MethodPost:
		postData(w, r)
	case http.MethodPut:
		// Update an existing record.
	case http.MethodDelete:
		deleteJob(w, r)
	default:
		// Give an error message.
	}

}
