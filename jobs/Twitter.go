package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/sirupsen/logrus"
	"injester_test/settings"
	"log"
	"strings"
	"sync"
	"time"
)

type TweetJob struct {
	JobJson
	TSearch     string `json:"search"`
	SearchParam []string

	Client       *twitter.Client
	Demux        twitter.SwitchDemux
	FilterParams *twitter.StreamFilterParams

	Downloads *chan string
	Output    *chan Model

	Logger *logrus.Logger
	Count  int
}

type TwitterStruct struct {
	*twitter.Tweet
	InjectTime time.Time `json:"inject_time"`
}

func (t TwitterStruct) Type() string {
	return "twitter"
}

func (j TweetJob) New(jb JobJson) *TweetJob {

	j = TweetJob{}
	j.ID = jb.ID
	j.Name = jb.Name
	j.Time = jb.Time
	j.Type = jb.Type

	err := json.Unmarshal([]byte(jb.Params), &j)
	if err != nil {
		fmt.Println(err)
	}

	if j.TSearch != "" {
		j.SearchParam = strings.Split(j.TSearch, ",")
	}

	config := oauth1.NewConfig("EsuGWiTLdWcZ8jv9vq3sEwwjw", "uDkeSzncRyX2ZjEQYqKA2CiwqgrCoUQReSIOEKnC9uTh8IUcFn")
	token := oauth1.NewToken("18946438-6snllPF33w1pxOt3KWPgo430jaPGtvTOd3mXN7b35", "tsgxnNaDIdgRAOh7WWVvqvNPds5XQC1cstqpWjUenmxdh")
	httpClient := config.Client(oauth1.NoContext, token)

	j.Client = twitter.NewClient(httpClient)
	j.Demux = twitter.NewSwitchDemux()

	//j.Demux.Tweet = ProcessTweet
	return &j
}

func (a *TweetJob) Init(ectd *clientv3.Client, log *logrus.Logger, settings *settings.SettingStore, downloads *chan string, out *chan Model) {
	a.Downloads = downloads
	a.Output = out
	a.Logger = log

	a.FilterParams = &twitter.StreamFilterParams{
		Track:         a.SearchParam, //[]string{"cat","dog","rabbit"},
		StallWarnings: twitter.Bool(true),
	}
	fmt.Println(a.TSearch)
	log.Info("JOB: " + a.Name + " Created")
}

func (job *TweetJob) GetCount() int {
	return job.Count
}

func (a *TweetJob) Run(ctx context.Context, wg *sync.WaitGroup) {
	// tell the caller we've stopped
	defer wg.Done()
	config := oauth1.NewConfig("EsuGWiTLdWcZ8jv9vq3sEwwjw", "uDkeSzncRyX2ZjEQYqKA2CiwqgrCoUQReSIOEKnC9uTh8IUcFn")
	token := oauth1.NewToken("18946438-6snllPF33w1pxOt3KWPgo430jaPGtvTOd3mXN7b35", "tsgxnNaDIdgRAOh7WWVvqvNPds5XQC1cstqpWjUenmxdh")
	httpClient := config.Client(oauth1.NoContext, token)
	client := twitter.NewClient(httpClient)
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		a.Count++
		*a.Output <- TwitterStruct{tweet, time.Now()}
	}
	a.Logger.Info("Starting Stream...")

	stream, err := client.Streams.Filter(a.FilterParams)
	if err != nil {
		log.Fatal(err)
	}

	defer stream.Stop()
	for {
		select {
		case message := <-stream.Messages:
			//fmt.Println("=========== HANDLE:"+a.Name+"==============");
			demux.Handle(message)
		case <-ctx.Done():
			a.Logger.Info(a.Name + ": caller has told us to stop\n")
			return
		}
	}
}
