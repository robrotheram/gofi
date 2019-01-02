package pipeline

import (
	"context"
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"log"
	"sync"
)

type ArticlePipeline struct {
	channel  string
	producer *nsq.Producer
	consumer *nsq.Consumer
	settings PipelineSettings

	inputTopic  string
	outputTopic string

	receivedCount int
	sendCount     int

	URL string `json:url`
}

func init() {
	RegisterPipeLine(func() Pipeline {
		return new(ArticlePipeline)
	})
}

func (a *ArticlePipeline) GetType() string {
	return "ARTICLE"
}

func (a ArticlePipeline) GetParams() *PipeLineParams {
	return nil
}

func (a *ArticlePipeline) GetCount() (int, int, string) {
	return a.sendCount, a.receivedCount, ""
}

func (w ArticlePipeline) New() *ArticlePipeline {
	return &w
}

func (p *ArticlePipeline) Init(settings PipelineSettings, config PipeLineJson) {
	p.settings = settings
	//p.inputTopic = config.InputTopic
	p.outputTopic = config.OutputTopic

	u2 := uuid.NewV4()
	p.channel = u2.String()
}

func (p *ArticlePipeline) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	q, _ := nsq.NewConsumer(p.inputTopic, p.channel, nsq.NewConfig())
	q.AddHandler(p)
	err := q.ConnectToNSQD(p.settings.NsqAddr)
	p.producer, _ = nsq.NewProducer(p.settings.NsqAddr, nsq.NewConfig())

	if err != nil {
		log.Panic("Could not connect")
	}
	<-ctx.Done()
	p.close()
}

func (p *ArticlePipeline) close() {
	p.producer.Stop()

	//TODO Delete Channel need to use HTTP to NSQ See https://github.com/nsqio/nsq/blob/8f6fa1f436a592e609d3168d50f6896486775d03/internal/clusterinfo/data.go#L758
}

func (p *ArticlePipeline) send(message string) {
	p.producer.Publish(p.outputTopic, []byte(message))

}

func (*ArticlePipeline) HandleMessage(msg *nsq.Message) error {

	// Handles input
	fmt.Println(msg.Attempts)
	fmt.Println(msg.ID)
	fmt.Println(msg.NSQDAddress)
	fmt.Println(msg.Timestamp)
	fmt.Println(string(msg.Body))

	return nil
}
