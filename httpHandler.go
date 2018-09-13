package main

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"html/template"
	"injester_test/jobs"
	"injester_test/settings"
	"log"
	"net/http"
)

type PageVariables struct {
	Jobs    []jobs.JobJson
	Metric  *jobs.Metric
	Time    string
	Service []jobs.Metric
}

func setupServer() {
	http.HandleFunc("/", HomePage)
	Logger.Info("http://localhost:8083" +
		"")
	Logger.Error(http.ListenAndServe(":8083", nil))
}

func HomePage(w http.ResponseWriter, r *http.Request) {

	HomePageVars := PageVariables{}
	resps, err := EClient.Get(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_SERVICE, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		log.Fatal(err)
	} else {
		for _, v := range resps.Kvs {
			m := jobs.Metric{}
			err := json.Unmarshal((v.Value), &m)
			if err == nil {
				HomePageVars.Service = append(HomePageVars.Service, m)
				for _, v := range m.Jobs {
					v.ID = m.Hostname
					v.Time = m.Time.Format("2006-01-02 15:04:05")
					HomePageVars.Jobs = append(HomePageVars.Jobs, v)
				}
			}
		}
	}

	HomePageVars.Metric = Metrics

	t, err := template.ParseFiles("templates/index.html") //parse the html file homepage.html
	if err != nil {                                       // if there is an error
		log.Print("template parsing error: ", err) // log it
	}
	err = t.Execute(w, HomePageVars) //execute the template and pass it the HomePageVars struct to fill in the gaps
	if err != nil {                  // if there is an error
		log.Print("template executing error: ", err) //log it
	}
}
