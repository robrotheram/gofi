package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/fsnotify/fsnotify"
	"injester_test/jobs"
	"injester_test/settings"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

type PageVariables struct {
	Jobs    []jobs.JobJson
	Metric  *jobs.Metric
	Time    string
	Service []jobs.Metric
}

func setupServer() {
	BuildUI()
	fs := http.FileServer(http.Dir("public"))

	http.HandleFunc("/data", HomePage)
	http.Handle("/", fs)
	Logger.Info("http://localhost:8083" +
		"")
	Logger.Error(http.ListenAndServe(":8083", nil))
}

func getData(w http.ResponseWriter, r *http.Request) {
	HomePageVars := PageVariables{}
	HomePageVars.Metric = Metrics
	HomePageVars.Jobs = TmpJobList

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
	var job jobs.JobJson
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
	fmt.Println(jobList)
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

func printCommand(cmd *exec.Cmd) {
	fmt.Printf("==> Executing: %s\n", strings.Join(cmd.Args, " "))
}

func printError(err error) {
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("==> Error: %s\n", err.Error()))
	}
}

func printOutput(outs []byte) {
	if len(outs) > 0 {
		fmt.Printf("==> Output: %s\n", string(outs))
	}
}

func BuildUI() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					cmd := exec.Command("npm", "run", "build")
					cmd.Dir = "./ui"
					cmdOutput := &bytes.Buffer{}
					cmd.Stdout = cmdOutput
					cmd.Run()
					if len(cmdOutput.Bytes()) > 0 {
						fmt.Printf("==> Output: %s\n", string(cmdOutput.Bytes()))
					}
					log.Println("BUILD COMPLETE")
				}
			case _, ok := <-watcher.Errors:
				if !ok {
					return
				}
			}
		}
	}()
	err = watcher.Add("./ui/src/")
	if err != nil {
		log.Fatal(err)
	}
}
