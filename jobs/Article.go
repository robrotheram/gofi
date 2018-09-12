package jobs

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/advancedlogic/GoOse"
	"github.com/coreos/etcd/clientv3"
	"github.com/mmcdole/gofeed"
	"github.com/sirupsen/logrus"
	"injester_test/settings"
	"runtime"
	"sync"
	"time"
)

type ArticleJob struct {
	JobJson
	URL       string `json:url`
	FeedTitle string
	Publisher string
	CachedUrl []string

	Setting      *settings.SettingStore
	EClient      *clientv3.Client
	Logger       *logrus.Logger
	GooseClient  goose.Goose
	GoFeedClient *gofeed.Parser

	Downloads *chan string
	Output    *chan Model
}

func (a ArticleStruct) Type() string {
	return "article"
}

type ArticleStruct struct {
	Publisher   string
	Title       string
	Content     string
	Description string
	TopImage    string
	TopImageID  string
	S3ID        string
	Url         string
	Keywords    string
	PublishDate time.Time
	InjectTime  time.Time
}

func (a ArticleJob) New(j JobJson) *ArticleJob {
	a = ArticleJob{}
	a.ID = j.ID
	a.Name = j.Name
	a.Type = j.Type
	a.Params = j.Params
	a.Time = j.Time

	err := json.Unmarshal([]byte(j.Params), &a)
	if err != nil {
		fmt.Println(err)
	}

	return &a
}

func (a *ArticleJob) Init(ectd *clientv3.Client, log *logrus.Logger, settings *settings.SettingStore, downloads *chan string, out *chan Model) {
	a.EClient = ectd
	a.Logger = log

	a.GoFeedClient = gofeed.NewParser()
	a.GooseClient = goose.New()
	a.Downloads = downloads
	a.Output = out
	a.Setting = settings
	log.Info("JOB: " + a.Name + " Created")
}

func (c *ArticleJob) Clear() {
	c = nil
	runtime.GC()
}

func (a *ArticleJob) GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (job *ArticleJob) AppendURL(url string) {
	job.Logger.Info(settings.ETCD_ROOT + "/" + settings.ETCD_URL + "/" + job.FeedTitle + "/" + url)
	job.EClient.Put(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_URL+"/"+job.FeedTitle+"/"+url, url)
}

func (job ArticleJob) ConstainsUrl(url string) bool {
	resp, err := job.EClient.Get(context.Background(), settings.ETCD_ROOT+"/"+settings.ETCD_URL+"/"+job.FeedTitle+"/"+url)
	if err != nil {
		return false
	}
	return (len(resp.Kvs) != 0)
}

func (job ArticleJob) getArticles(item *gofeed.Item) {
	t := time.Now()
	article, _ := job.GooseClient.ExtractFromURL(item.Link)
	if article == nil {
		return
	}

	*job.Output <- ArticleStruct{
		job.FeedTitle,
		article.Title,
		article.CleanedText,
		article.MetaDescription,
		article.TopImage,
		job.GetMD5Hash(article.TopImage),
		"http://" + job.Setting.Endpoint + "/minio/download/" + job.Setting.BucketName + "/" + (job.GetMD5Hash(article.TopImage)) + "?token=%27%27",
		article.FinalURL,
		article.MetaKeywords,
		t,
		t,
	}

	if article.TopImage != "" {
		*job.Downloads <- article.TopImage
	}

	job.Logger.Info("URL Cache:", len(job.CachedUrl))
}

func (job *ArticleJob) Run(ctx context.Context, wg *sync.WaitGroup) {
	// tell the caller we've stopped
	defer wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			job.Logger.Info(fmt.Sprintf("%s processing at %s \n", job.Name, now.UTC().Format("2006-01-02 15:04:05-07:00")))
			feed, err := job.GoFeedClient.ParseURL(job.URL)

			if err != nil {
				job.Logger.Info("ERROR GETTING: " + job.URL)
				job.Logger.Error(err)
				return
			}

			job.FeedTitle = feed.Title

			for _, v := range feed.Items {
				if !job.ConstainsUrl(v.Link) {
					job.Logger.Info("Aritical added: " + v.Link)
					job.getArticles(v)
					job.AppendURL(v.Link)
				}
			}
		case <-ctx.Done():
			job.Logger.Info(job.Name + ": caller has told us to stop\n")
			return
		}
	}
}
