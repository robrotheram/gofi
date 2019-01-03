package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/olivere/elastic"
	"github.com/robrotheram/gofi/api"
	"github.com/robrotheram/gofi/datastore"
	"github.com/robrotheram/gofi/leaderElection"
	"github.com/robrotheram/gofi/pipeline"
	"github.com/robrotheram/gofi/scheduler"
	"github.com/robrotheram/gofi/settings"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

var (
	EClient   *clientv3.Client
	ESclient  *elastic.Client
	Logger    *logrus.Logger
	Downloads = make(chan string, 100)
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
	settings.NodeIP, _ = getIP()

	if settings.Debug {
		settings.SetHostname(GetRandomName(0))
	} else {
		hostname, _ := os.Hostname()
		settings.SetHostname(hostname)
	}
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

	if leaderElection.Election.IsLeader() {
		//If I am the leader then then start nsq
		go pipeline.StartNSQ()
	}
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
		scheduler.Orchestrator.Load()
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			updateHealth()
			if leaderElection.Election.IsLeader() {
				if scheduler.Orchestrator.Size() == 0 {
					scheduler.Orchestrator.Load()
				}
				if !scheduler.CheckHealth() {
					fmt.Println("REBALANCING!!!!")
					scheduler.Orchestrator.Rebalance()
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

func getIP() (string, error) {
	host, _ := os.Hostname()
	addr, err := net.LookupIP(host)
	if err != nil {
		return "", err
	}
	return addr[0].String(), nil
}
