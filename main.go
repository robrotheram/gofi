package main

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
	"injester_test/jobs"
	"injester_test/settings"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"
)

var (
	EClient   *clientv3.Client
	ESclient  *elastic.Client
	Settings  *settings.SettingStore
	Logger    *logrus.Logger
	Metrics   *settings.Metric
	Downloads = make(chan string, 100)
	Output    = make(chan jobs.Model, 100)
)

func init() {
	rand.Seed(time.Now().UnixNano())
	Settings = new(settings.SettingStore)
	Metrics = new(settings.Metric)

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

	Settings.Print(Logger)
	Logger.Info("=========================================== ")

}

func main() {
	InitScheduler()
	RunCoreJob(ServiceJob{}.New())
	RunCoreJob(WatcherJob{}.New())
	RunCoreJob(DownloadJob{}.New())
	RunCoreJob(ElasticJob{}.New())
	go func() {
		setupServer()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	Logger.Info("main: received C-c - shutting down")
	StopScheduler()
}
