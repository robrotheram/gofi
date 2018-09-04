package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go"
	"github.com/mmcdole/gofeed"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"time"
)

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

func (s ArticleStruct) print() {
	e := reflect.ValueOf(&s).Elem()
	for i := 0; i < e.NumField(); i++ {
		fieldName := e.Type().Field(i).Name
		fieldValue := e.Field(i)
		Logger.WithFields(logrus.Fields{
			fieldName: fieldValue,
		}).Info("Config:")
	}
}

type ArticleJob struct {
	URL       string `json:url`
	FeedTitle string
	CachedUrl []string
	*gofeed.Item
	Publisher string
}

func (c *ArticleJob) Clear() {
	c = nil
	runtime.GC()
}

func (c *ArticleJob) AppendURL(url string) {
	Logger.Info(ETCD_ROOT + "/" + ETCD_URL + "/" + c.FeedTitle + "/" + url)
	EClient.Put(context.Background(), ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle+"/"+url, url)
}

func (c ArticleJob) ConstainsUrl(url string) bool {
	resp, err := EClient.Get(context.Background(), ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle+"/"+url)
	if err != nil {
		return false
	}
	return (len(resp.Kvs) != 0)
}

func (af *ArticleJob) FromJob(j Job) error {
	err := json.Unmarshal([]byte(j.Params), af)
	return err
}

func (af *ArticleJob) Process() {
	Logger.Info("Parsing URL:" + af.URL)
	feed, err := GoFeedClient.ParseURL(af.URL)

	if err != nil {
		fmt.Println(err)
		return
	}
	if Settings.Debug {
		Logger.Info(fmt.Sprintf("%s has count %d \n", feed.Title, len(feed.Items)))
	}
	af.FeedTitle = feed.Title

	for _, v := range feed.Items {
		if !af.ConstainsUrl(v.Link) {
			fmt.Println("Aritical added: " + v.Link)
			af.getArticles(v)
			af.AppendURL(v.Link)
		}
	}

}

func (af ArticleJob) getArticles(item *gofeed.Item) {
	t := time.Now()
	article, _ := GooseClient.ExtractFromURL(item.Link)

	if article == nil {
		return
	}

	sendToElastic(ArticleStruct{
		af.FeedTitle,
		article.Title,
		article.CleanedText,
		article.MetaDescription,
		article.TopImage,
		GetMD5Hash(article.TopImage),
		"http://" + Settings.Endpoint + "/minio/download/" + Settings.BucketName + "/" + (GetMD5Hash(article.TopImage)) + "?token=%27%27",
		article.FinalURL,
		article.MetaKeywords,
		t,
		t,
	})

	if article.TopImage != "" {
		downloads <- article.TopImage
	}

	Logger.Info("URL Cache:", len(af.CachedUrl))
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func downloadworker(downloads <-chan string) {
	for j := range downloads {
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
		_, err = minioClient.FPutObject(Settings.BucketName, (GetMD5Hash(j)), "/tmp/tmp.cache", minio.PutObjectOptions{ContentType: contentType})
		if err != nil {
			Logger.Error(err)
		}

	}
}
