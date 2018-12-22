package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
	"injester_test/api"
	"injester_test/datastore"
	"injester_test/jobs"
	"injester_test/leaderElection"
	"injester_test/scheduler"
	"injester_test/settings"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"
)

var (
	EClient   *clientv3.Client
	ESclient  *elastic.Client
	Logger    *logrus.Logger
	Metrics   *jobs.Metric
	Downloads = make(chan string, 100)
	Output    = make(chan jobs.Model, 100)
	Events    = make(chan string, 100)
)

func init() {
	rand.Seed(time.Now().UnixNano())
	settings := settings.Settings
	//	Logger.Info("Attempting to load config from file")
	settings.GetFromFile()
	settings.GetFromEnviroment()
	Logger = settings.GetLogger()
	Logger.Info("===== Config Loaded sucessfully  ========== ")

	if settings.Debug {
		settings.SetHostname(GetRandomName(0))
	} else {
		hostname, _ := os.Hostname()
		settings.SetHostname(hostname)
	}

	Metrics = jobs.Metric{}.New(settings.Hostname)
	settings.Print(Logger)
	Logger.Info("=========================================== ")

}

var datastore *Datastore.DataStore

func main() {
	//InitScheduler()
	datastore = Datastore.NewDataStore()
	defer datastore.Close()
	cleanUpDatabase()
	leaderElection.NewElection()
	leaderElection.Election.Start()
	defer leaderElection.Election.Stop()
	go healthCheck()
	scheduler.CreateScheduler(datastore)

	//go Shedular.Run(Events)

	api.SetupServer(datastore)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	Logger.Info("main: received C-c - shutting down")
	scheduler.Scheduler.StopAll()
	//StopScheduler()

}

func healthCheck() {
	updateHealth()
	if leaderElection.Election.IsLeader() {
		scheduler.ControllerProcess()
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			updateHealth()
			if leaderElection.Election.IsLeader() {
				if len(scheduler.Controller) == 0 {
					scheduler.ControllerProcess()
				}
				if !scheduler.CheckHealth() {
					fmt.Println("REBALANCING!!!!")
					scheduler.Rebalance()
				}
			}
		}
	}
}

func updateHealth() {
	scheduler.Health[settings.Settings.Hostname] = scheduler.GetMetrics()
	if !leaderElection.Election.IsLeader() {
		url := "http://" + leaderElection.Election.LeaderIP() + ":8000/controller/health"
		b, err := json.Marshal(scheduler.GetMetrics())
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
		} else {
			defer resp.Body.Close()
		}

	}
}

func cleanUpDatabase() {
	graphs, ok := datastore.Tables("GRAPH").GetAll().([]Datastore.Graph)
	if ok {
		for _, graph := range graphs {
			for i, item := range graph.Nodes {
				if item.ID == "" {
					graph.Nodes = append(graph.Nodes[:i], graph.Nodes[i+1:]...)
				}
			}
			for j, item := range graph.Connections {
				found := false
				for _, node := range graph.Nodes {
					if item.InputNode == node.ID || item.OutputNode == node.ID {
						found = true
					}
				}
				if !found {
					if len(graph.Connections) > 1 {
						graph.Connections = append(graph.Connections[:j], graph.Connections[j+1:]...)
					} else {
						graph.Connections = []Datastore.Connection{}
					}
				}
			}
			datastore.Tables("GRAPH").Save(graph)
		}
	}

}
