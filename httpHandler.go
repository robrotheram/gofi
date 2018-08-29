package main

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"html/template"
	"log"
	"net/http"
)

type PageVariables struct {
	Jobs    []Job
	Metric  Metric
	Time    string
	Service []Metric
}

func setupServer() {
	http.HandleFunc("/", HomePage)
	Logger.Info("http://localhost:8088")
	Logger.Error(http.ListenAndServe(":8083", nil))
}

func HomePage(w http.ResponseWriter, r *http.Request) {

	HomePageVars := PageVariables{}
	resps, err := EClient.Get(context.Background(), ETCD_ROOT+"/"+ETCD_SERVICE, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		log.Fatal(err)
	} else {
		for _, v := range resps.Kvs {
			m := Metric{}
			err := json.Unmarshal((v.Value), &m)
			if err == nil {
				HomePageVars.Service = append(HomePageVars.Service, m)
				HomePageVars.Jobs = append(HomePageVars.Jobs, m.Job...)
			}
		}
	}

	Metrics.Update()

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
