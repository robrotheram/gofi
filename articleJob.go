package main

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/advancedlogic/GoOse"
	"github.com/minio/minio-go"
	"github.com/mmcdole/gofeed"
	"github.com/sirupsen/logrus"
	"net/http"
	"reflect"
	"strings"
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
	CachedAricles []ArticleStruct
	CachedUrl     []string
	*gofeed.Item
	Publisher string
}

func (c *ArticleJob) AppendURL(url string) {
	c.CachedUrl = append(c.CachedUrl, url)
	EClient.Put(context.Background(), ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle, strings.Join(c.CachedUrl[:], ","))
}

func (c *ArticleJob) getURLCache() {
	resp, err := EClient.Get(context.Background(), ETCD_ROOT+"/"+ETCD_URL+"/"+c.FeedTitle)
	if err != nil {
		return
	}
	if len(resp.Kvs) == 0 {
		return
	}
	c.UpdateURL(string(resp.Kvs[0].Value))
}

func (c *ArticleJob) SetURL(url []string) {
	c.CachedUrl = url
}

func (c *ArticleJob) UpdateURL(url string) {
	//Logger.Info("Updating....", url)
	s := strings.Split(url, ",")
	c.CachedUrl = s
}
func (c *ArticleJob) ConstainsUrl(url string) bool {
	for _, v := range c.CachedUrl {
		if v == url {
			return true
		}
	}
	return false
}

func (af *ArticleJob) FromJob(j Job) error {
	err := json.Unmarshal([]byte(j.Params), af)
	return err
}

func (af *ArticleJob) Process() {
	fp := gofeed.NewParser()
	Logger.Info("Parsing URL:" + af.URL)
	feed, err := fp.ParseURL(af.URL)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s has count %d \n", feed.Title, len(feed.Items))
	af.FeedTitle = feed.Title
	af.getURLCache()

	for _, v := range feed.Items {
		h := sha1.New()
		h.Write([]byte(v.Link))
		if !af.ConstainsUrl(v.Link) {
			fmt.Println("Aritical added: " + v.Link)
			af.getArticles(v)
			af.AppendURL(v.Link)
		}
	}

}

func (af *ArticleJob) getArticles(item *gofeed.Item) {
	g := goose.New()
	t := time.Now()
	article, _ := g.ExtractFromURL(item.Link)

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

func downloadworker(id int, downloads <-chan string, results chan<- string) {
	for j := range downloads {
		url := j
		// don't worry about errors
		response, e := http.Get(url)
		if e != nil {
			return
		}

		defer response.Body.Close()
		minioClient.PutObject(Settings.BucketName, (GetMD5Hash(j) + ".jpg"), response.Body, -1, minio.PutObjectOptions{ContentType: "image/jpg"})

		//open a file for writing
		//file, err := os.Create("tmp/"+GetMD5Hash(j)+".jpg")
		//if err != nil {
		//	return
		//}
		//// Use io.Copy to just dump the response body to the file. This supports huge files
		//_, err = io.Copy(file, response.Body)
		//if err != nil {
		//	return
		//}
		//file.Close()
	}
}
