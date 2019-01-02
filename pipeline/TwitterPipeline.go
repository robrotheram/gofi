package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

type TwitterPipeline struct {
	channel     string
	producer    *nsq.Producer
	consumer    *nsq.Consumer
	settings    PipelineSettings
	inputTopic  string
	outputTopic string

	receivedCount int
	sendCount     int

	searchParam  []string
	client       *twitter.Client
	demux        twitter.SwitchDemux
	filterParams *twitter.StreamFilterParams
	httpClient   *http.Client

	TSearch        string `json:"search"`
	ConsumerKey    string `json:"consumerKey"`
	ConsumerSecret string `json:"consumerSecret"`
	Token          string `json:"token"`
	TokenSecret    string `json:"tokenSecret"`
}

func init() {
	RegisterPipeLine(func() Pipeline {
		return new(TwitterPipeline)
	})
}

func (a *TwitterPipeline) GetType() string {
	return "TWITTER"
}

func (s TwitterPipeline) GetCount() (int, int, string) {
	return s.sendCount, s.receivedCount, ""
}

func (p *TwitterPipeline) GetConfig() interface{} {
	return p
}

func (w TwitterPipeline) New() *TwitterPipeline {
	return &w
}

func (p *TwitterPipeline) setConfig(params string) error {
	var anyJson map[string]interface{}
	err := json.Unmarshal([]byte(params), &anyJson)
	if err != nil {
		return err
	}
	p.TSearch = anyJson["search"].(string)
	p.ConsumerKey = anyJson["consumerKey"].(string)
	p.ConsumerSecret = anyJson["consumerSecret"].(string)
	p.Token = anyJson["token"].(string)
	p.TokenSecret = anyJson["tokenSecret"].(string)
	return nil
}

func (p *TwitterPipeline) Init(settings PipelineSettings, config PipeLineJson) {
	p.settings = settings
	//p.inputTopic = config.InputTopic
	p.outputTopic = config.OutputTopic
	p.setConfig(config.Params)
	if p.TSearch != "" {
		p.searchParam = strings.Split(p.TSearch, ",")
	}

	cnfg := oauth1.NewConfig(p.ConsumerKey, p.ConsumerSecret)
	token := oauth1.NewToken(p.Token, p.TokenSecret)
	p.httpClient = cnfg.Client(oauth1.NoContext, token)

	p.client = twitter.NewClient(p.httpClient)
	p.demux = twitter.NewSwitchDemux()

	u2 := uuid.NewV4()

	p.channel = u2.String()
}

func (p *TwitterPipeline) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("Starting TWITTER")

	p.producer, _ = nsq.NewProducer(p.settings.NsqAddr, nsq.NewConfig())
	p.producer.SetLogger(log.New(os.Stderr, "", log.Flags()), nsq.LogLevelWarning)
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		if tweet.Text != "" && len(tweet.Text) > 10 {
			p.send(tweet.Text)
		}

	}

	stream, err := p.client.Streams.Filter(&twitter.StreamFilterParams{
		Track:         p.searchParam, //[]string{"cat","dog","rabbit"},
		StallWarnings: twitter.Bool(true),
	})
	if err != nil {
		log.Fatal(err)
	}

	defer stream.Stop()
	for {
		select {
		case message := <-stream.Messages:
			demux.Handle(message)
		case <-ctx.Done():
			fmt.Println("time to stop now")
			return
		}
	}
}

func (p *TwitterPipeline) close() {

	p.producer.Stop()

	//TODO Delete Channel need to use HTTP to NSQ See https://github.com/nsqio/nsq/blob/8f6fa1f436a592e609d3168d50f6896486775d03/internal/clusterinfo/data.go#L758
}

func (p *TwitterPipeline) send(message string) {
	p.sendCount++
	p.producer.Publish(p.outputTopic, []byte(message))

}

func (tw *TwitterPipeline) HandleMessage(msg *nsq.Message) error {

	// Handles input
	fmt.Println(msg.Attempts)
	fmt.Println(msg.ID)
	fmt.Println(msg.NSQDAddress)
	fmt.Println(msg.Timestamp)
	fmt.Println(string(msg.Body))

	tw.send(string(msg.Body))
	return nil
}
