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
	Metric  *settings.Metric
	Time    string
	Service []settings.Metric
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
			m := settings.Metric{}
			err := json.Unmarshal((v.Value), &m)
			if err == nil {

				HomePageVars.Service = append(HomePageVars.Service, m)
				//for _, v := range m.Job {
				//	v.ID = m.Hostname
				//	HomePageVars.Jobs = append(HomePageVars.Jobs, v)
				//}
			}
		}
	}

	Metrics.Update(Settings)
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
