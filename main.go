package main

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/minio/minio-go"
	"github.com/sirupsen/logrus"
	"log"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"
)

var (
	Logger = logrus.New()
)

func setupLogger() {
	switch output := Settings.LogOutut; output {
	case LOG_STD_OUT:
		log.SetOutput(os.Stdout)
	case LOG_FILE_OUT:
		f, err := os.OpenFile("logrus.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize log file %s", err)
			os.Exit(1)
		}
		Logger.Out = f
	default:
		log.SetOutput(os.Stdout)
	}
	switch level := Settings.LogLevel; level {
	case LOG_INFO:
		Logger.Level = logrus.InfoLevel
	case LOG_DEBUG:
		Logger.Level = logrus.DebugLevel
	case LOG_WARN:
		Logger.Level = logrus.WarnLevel
	case LOG_ERROR:
		Logger.Level = logrus.ErrorLevel
	default:
		Logger.Level = logrus.InfoLevel
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
	Settings = new(SettingStore)
	Logger.Info("Attempting to load config from file")
	getFromFile()
	getFromEnviroment()
	setupLogger()
	Logger.Info("===== Config Loaded sucessfully  ========== ")

	if Settings.Debug {
		Settings.SetHostname(GetRandomName(0))
	} else {
		hostname, _ := os.Hostname()
		Settings.SetHostname(hostname)
	}

	Settings.print()
	Logger.Info("=========================================== ")
}

func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func updateSettings() {
	resp, err := EClient.Grant(context.Background(), 60)
	if err != nil {
		log.Fatal(err)
	}
	Metrics.Update()
	_, err = EClient.Put(context.Background(), ETCD_ROOT+"/"+ETCD_SERVICE+"/"+Settings.Hostname, Metrics.ToString(), clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}
}

func printKeys() {
	resps, err := EClient.Get(context.Background(), "e3w_test", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		log.Fatal(err)
	} else {
		//log.Printf("Get is done. Metadata is %q\n", resps)
		for _, v := range resps.Kvs {
			Logger.WithFields(logrus.Fields{
				string(v.Key): string(v.Value),
			}).Info("Config:")

		}

	}
}
func main() {
	//Setup etcD

	e, err := clientv3.New(clientv3.Config{
		Endpoints:   Settings.Ectd,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		Logger.Error(err)
		os.Exit(1)
	}
	EClient = e
	Logger.Info("connected to etcd - " + strings.Join(Settings.Ectd[:], ","))
	EClient.Delete(context.Background(), ETCD_ROOT+"/"+ETCD_JOB, clientv3.WithPrefix())
	//EClient.Delete(context.Background(), ETCD_ROOT+"/"+ETCD_URL, clientv3.WithPrefix())
	defer EClient.Close()

	mc, err := minio.New(Settings.Endpoint, Settings.AccessKeyID, Settings.SecretAccessKey, Settings.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}
	minioClient = mc
	Metrics.Update()
	createBuket()
	createClient()
	updateSettings()

	time.Sleep(2 * time.Second)
	setupWatchers()

	sigs := make(chan os.Signal, 1)
	// catch all signals since not explicitly listing
	signal.Notify(sigs)
	//signal.Notify(sigs,syscall.SIGQUIT)
	// method invoked upon seeing signal
	go func() {
		s := <-sigs
		Logger.Info("RECEIVED SIGNAL:" + s.String())
		EClient.Delete(context.Background(), ETCD_ROOT+"/"+ETCD_SERVICE+"/"+Settings.Hostname, clientv3.WithPrefix())
		os.Exit(1)
	}()

	go downloadworker(downloads)

	go func() {
		setupServer()
	}()

	for {
		for i, job := range JobList {
			if i >= len(JobList) {
				break
			}
			job.parseJob()

		}

		runtime.GC()
		updateSettings()
		time.Sleep(time.Millisecond * time.Duration(60000))
	}

}
