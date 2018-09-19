package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/minio/minio-go"
	"github.com/olivere/elastic"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"injester_test/jobs"
	"injester_test/settings"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type ServiceJob struct {
	Name   string
	Metric *jobs.Metric
	Count  int
}

func (s ServiceJob) New(metric *jobs.Metric) *ServiceJob {
	s.Name = "Service Status"
	s.Metric = metric
	return &s
}

func (s ServiceJob) GetCount() int {
	return s.Count
}

func (c *ServiceJob) Init(ectd *clientv3.Client, log *logrus.Logger, settings *settings.SettingStore, downloads *chan string, out *chan jobs.Model) {
	Logger.Info(fmt.Sprintf("job: %s Is Initiation", c.Name))
	c.updateETCD()
	c.UpdatedMetrics()
}
func (c *ServiceJob) Stop() {}

func (c *ServiceJob) UpdatedMetrics() {
	c.Metric.Update()
	count := 0
	c.Metric.JobsProcessed = 0
	for _, job := range JobList {
		count += job.GetCount()
	}
	c.Metric.JobsProcessed = count - c.Metric.JobsProcessed
}

func (c *ServiceJob) updateETCD() {
	resp, err := EClient.Grant(context.Background(), 60)
	if err != nil {
		log.Fatal(err)
	}
	c.UpdatedMetrics()
	_, err = EClient.Put(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_SERVICE+"/"+Settings.Hostname, c.Metric.ToString(), clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}
}

func (c *ServiceJob) Run(ctx context.Context, wg *sync.WaitGroup) {
	// tell the caller we've stopped
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	Logger.Info(fmt.Sprintf("job: %s Is Starting", c.Name))
	for {
		select {
		case now := <-ticker.C:
			Logger.Info(fmt.Sprintf("%s processing at %s", c.Name, now.UTC().Format("2006-01-02 15:04:05-07:00")))
			c.updateETCD()
			c.Count++
		case <-ctx.Done():
			Logger.Info(fmt.Sprintf("%s: Stopping...", c.Name))
			return
		}
	}
}

func (w WatcherJob) New() *WatcherJob {
	w.Name = "Watcher Job"
	return &w
}

type WatcherJob struct {
	Name  string
	Count int
}

func (s WatcherJob) GetCount() int {
	return s.Count
}

func (c *WatcherJob) Init(ectd *clientv3.Client, log *logrus.Logger, set *settings.SettingStore, downloads *chan string, out *chan jobs.Model) {
	resps, err := EClient.Get(context.Background(), "e3w_test", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		log.Fatal(err)
	} else {
		for _, v := range resps.Kvs {
			if strings.HasSuffix(string(v.Key), settings.ETCD_JOB_LIST) {
				parseJobList(v.Value)
			}
		}
	}
}

func (c *WatcherJob) Stop() {}

func (c *WatcherJob) Run(ctx context.Context, wg *sync.WaitGroup) {
	watchChan := EClient.Watch(context.Background(), "e3w_test", clientv3.WithPrefix())
	defer wg.Done()
	Logger.Info(fmt.Sprintf("job: %s Is Starting", c.Name))
	for {
		select {
		case watchResp := <-watchChan:
			Logger.Info(fmt.Sprintf("%s processing at %s\n", c.Name, time.Now().UTC().Format("2006-01-02 15:04:05-07:00")))
			for _, event := range watchResp.Events {
				if (event.Type == clientv3.EventTypePut) || (event.Type == clientv3.EventTypeDelete) {
					Logger.Info("Updating Store key: " + string(event.Kv.Key))
					if strings.HasSuffix(string(event.Kv.Key), settings.ETCD_JOB_LIST) {
						parseJobList(event.Kv.Value)
					}
					if strings.HasPrefix(string(event.Kv.Key), settings.ETCD_ROOT+"/"+settings.ETCD_SERVICE) {
						numberofWorkers, _, _ := GetNumberOfWorkers()
						if CachedWorkers != numberofWorkers {
							CachedWorkers = numberofWorkers
							SetupJobs()
						}

					}
				}
			}
		case <-ctx.Done():
			Logger.Info(fmt.Sprintf("%s: Stopping...\n", c.Name))
			return
		}
	}
}

type DownloadJob struct {
	Name        string
	minioClient *minio.Client
	Count       int
}

func (w DownloadJob) New() *DownloadJob {
	w.Name = "Download Job"
	return &w
}

func (s DownloadJob) GetCount() int {
	return s.Count
}

func (c *DownloadJob) Init(ectd *clientv3.Client, log *logrus.Logger, set *settings.SettingStore, downloads *chan string, out *chan jobs.Model) {
	mc, err := minio.New(Settings.Endpoint, Settings.AccessKeyID, Settings.SecretAccessKey, Settings.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}
	err = mc.MakeBucket(Settings.BucketName, settings.MINO_BUCKET_LOCATION)
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, err := mc.BucketExists(Settings.BucketName)
		if err == nil && exists {
			Logger.Info("We already own %s\n", Settings.BucketName)
		} else {
			Logger.Error(err)
		}
	}

	policy := `{"Version": "2012-10-17","Statement": [{"Action": ["s3:GetObject"],"Effect": "Allow","Principal": {"AWS": ["*"]},"Resource": ["arn:aws:s3:::` + Settings.BucketName + `/*"],"Sid": ""}]}`

	err = mc.SetBucketPolicy(Settings.BucketName, policy)
	if err != nil {
		fmt.Println(err)
		return
	}
	Logger.Info("Successfully created %s\n", Settings.BucketName)
	c.minioClient = mc
}

func (c *DownloadJob) Stop() {}

func (c *DownloadJob) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	Logger.Info(fmt.Sprintf("job: %s Is Starting", c.Name))
	for {
		select {
		case j := <-Downloads:
			c.Count++
			response, e := http.Get(j)
			if e != nil {
				Logger.Error(e)
			}
			defer response.Body.Close()
			contentType := response.Header.Get("Content-Type")
			file, err := os.Create("/tmp/tmp.cache")
			if err != nil {
				Logger.Error(err)
			}
			_, err = io.Copy(file, response.Body)
			if err != nil {
				Logger.Error(err)
			}
			file.Close()
			hasher := md5.New()
			hasher.Write([]byte(j))
			_, err = c.minioClient.FPutObject(Settings.BucketName, (hex.EncodeToString(hasher.Sum(nil))), "/tmp/tmp.cache", minio.PutObjectOptions{ContentType: contentType})
			if err != nil {
				Logger.Error(err)
			}

		case <-ctx.Done():
			Logger.Info(fmt.Sprintf("%s: Stopping...\n", c.Name))
			return
		}
	}
}

type ElasticJob struct {
	Name     string
	ESclient *elastic.Client
	Count    int
}

func (w ElasticJob) New() *ElasticJob {
	w.Name = "Elastic Job"
	return &w
}

func (s ElasticJob) GetCount() int {
	return s.Count
}

func (c *ElasticJob) Init(ectd *clientv3.Client, log *logrus.Logger, set *settings.SettingStore, downloads *chan string, out *chan jobs.Model) {
	var err error
	c.ESclient, err = elastic.NewSimpleClient(elastic.SetURL(Settings.Elastic))
	if err != nil {
		log.Fatal(err)
	}
	Logger.Info("Successfully connected to Elastic: " + Settings.Elastic)
}

func (c *ElasticJob) Stop() {}

func (c *ElasticJob) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	Logger.Info(fmt.Sprintf("job: %s Is Starting", c.Name))
	for {
		select {
		case j := <-Output:
			c.Count++
			ctx := context.Background()
			bulkRequest := c.ESclient.Bulk()
			u2, err := uuid.NewV4()
			if err != nil {
				fmt.Printf("Something went wrong: %s", err)
				break
			}
			req := elastic.NewBulkIndexRequest().Index(j.Type()).Type(j.Type()).Id(u2.String()).Doc(j)
			bulkRequest = bulkRequest.Add(req)
			bulkResponse, err := bulkRequest.Do(ctx)
			if err != nil {
				fmt.Println(err)
			}
			if bulkResponse != nil {
			}
		case <-ctx.Done():
			Logger.Info(fmt.Sprintf("%s: Stopping...\n", c.Name))
			return
		}
	}
}
