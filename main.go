package main

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
	"injester_test/datastore"
	"injester_test/http"
	"injester_test/jobs"
	"injester_test/scheduler"
	"injester_test/settings"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

var (
	EClient   *clientv3.Client
	ESclient  *elastic.Client
	Settings  *settings.SettingStore
	Logger    *logrus.Logger
	Metrics   *jobs.Metric
	Downloads = make(chan string, 100)
	Output    = make(chan jobs.Model, 100)
	Events    = make(chan string, 100)
)

func init() {
	rand.Seed(time.Now().UnixNano())
	Settings = new(settings.SettingStore)

	//	Logger.Info("Attempting to load config from file")
	getFromFile()
	getFromEnviroment()
	Logger = settings.GetLogger(Settings)
	Logger.Info("===== Config Loaded sucessfully  ========== ")

	if Settings.Debug {
		Settings.SetHostname(GetRandomName(0))
	} else {
		hostname, _ := os.Hostname()
		Settings.SetHostname(hostname)
	}

	Metrics = jobs.Metric{}.New(Settings.Hostname)
	Settings.Print(Logger)
	Logger.Info("=========================================== ")

}

var datastore *Datastore.DataStore
var Shedular scheduler.Scheduler

func main() {
	//InitScheduler()
	datastore = Datastore.NewDataStore()
	defer datastore.Close()

	//cleanup
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

	Shedular = scheduler.CreateScheduler(datastore)
	Shedular.Process()

	//go Shedular.Run(Events)

	go func() {
		http.SetupServer(datastore, &Shedular)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	Logger.Info("main: received C-c - shutting down")
	Shedular.StopAll()
	//StopScheduler()

}
