package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/minio/minio-go"
	"github.com/mmcdole/gofeed"
	"github.com/sirupsen/logrus"
	"net/http"
	"reflect"
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
	URL           string `json:url`
	FeedTitle     string
	CachedUrl     []string
	*gofeed.Item
	Publisher string
}

func (c *ArticleJob) Clear() {
	c = nil
	runtime.GC()
}

func (c *ArticleJob) AppendURL(url string) {
	//c.CachedUrl = append(c.CachedUrl, url)
	Logger.Info( ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle+"/"+url)
	EClient.Put(context.Background(), ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle+"/"+url, url)
}

func (c *ArticleJob) getURLCache() {
	return
	//resp, err := EClient.Get(context.Background(), ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle)
	//if err != nil {
	//	return
	//}
	//if len(resp.Kvs) == 0 {
	//	return
	//}
	//c.UpdateURL(string(resp.Kvs[0].Value))
}

//func (c *ArticleJob) SetURL(url []string) {
//	c.CachedUrl = url
//}

//func (c *ArticleJob) UpdateURL(url string) {
//	if(Settings.Debug){
//		Logger.Info("Updating....", url)
//	}
//
//	s := strings.Split(url, ",")
//	c.CachedUrl = s
//}

func (c ArticleJob) ConstainsUrl(url string) bool {
	//for _, v := range c.CachedUrl {
	//	if v == url {
	//		return true
	//	}
	//}
	//return false

	resp, err := EClient.Get(context.Background(), ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle+"/"+url)
	if(err != nil){
		return false
	}
	return (len(resp.Kvs) != 0)
}

func (af *ArticleJob) FromJob(j Job) error {
	err := json.Unmarshal([]byte(j.Params), af)
	return err
}

func (af ArticleJob) Process() {
	Logger.Info("Parsing URL:" + af.URL)
	feed, err := GoFeedClient.ParseURL(af.URL)

	if err != nil {
		fmt.Println(err)
		return
	}
	if(Settings.Debug){
		Logger.Info(fmt.Sprintf("%s has count %d \n", feed.Title, len(feed.Items)))
	}
	af.FeedTitle = feed.Title
	//af.getURLCache()

	for _, v := range feed.Items {
		if !af.ConstainsUrl(v.Link) {
			fmt.Println("Aritical added: " + v.Link)
			af.getArticles(v)
			af.AppendURL(v.Link)
		}
	}

}

func (af ArticleJob) getArticles(item *gofeed.Item) {
	//return

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
		"http://" + Settings.Endpoint + "/minio/download/" + Settings.BucketName + "/" + (GetMD5Hash(article.TopImage) + ".jpg") + "?token=%27%27",
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
		url := j
		// don't worry about errors

		// I think there is a memory leak issue here. Allocats 600mb of ram for a split second then dissapears  currently disabled
		response, e := http.Get(url)
		if e != nil {
			return
		}
		minioClient.PutObject(Settings.BucketName, (GetMD5Hash(j) + ".jpg"), response.Body, -1, minio.PutObjectOptions{ContentType: "image/jpg"})
		response.Body.Close()
	}
}
