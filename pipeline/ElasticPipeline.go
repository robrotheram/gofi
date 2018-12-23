package pipeline

import (
	"context"
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"log"
	"sync"
)

type ElasticPipeline struct {
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
		return new(ElasticPipeline)
	})
}

func (a ElasticPipeline) GetParams() *PipeLineParams {
	return nil
}

func (a ElasticPipeline) GetType() string {
	return "ELASTICSEARCH"
}

func (s ElasticPipeline) GetCount() (int, int, string) {
	return s.sendCount, s.receivedCount, ""
}

func (w ElasticPipeline) New() *ElasticPipeline {
	return &w
}

func (p *ElasticPipeline) Init(settings PipelineSettings, config PipeLineJson) {
	p.settings = settings
	//p.inputTopic = config.InputTopic
	p.outputTopic = config.OutputTopic

	u2, err := uuid.NewV4()
	if err != nil {
		fmt.Printf("Something went wrong: %s", err)
		return
	}
	p.channel = u2.String()
}

func (p *ElasticPipeline) Run(ctx context.Context, wg *sync.WaitGroup) {
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

func (p *ElasticPipeline) close() {
	p.producer.Stop()

	//TODO Delete Channel need to use HTTP to NSQ See https://github.com/nsqio/nsq/blob/8f6fa1f436a592e609d3168d50f6896486775d03/internal/clusterinfo/data.go#L758
}

func (p *ElasticPipeline) send(message string) {
	p.producer.Publish(p.outputTopic, []byte(message))

}

func (*ElasticPipeline) HandleMessage(msg *nsq.Message) error {

	// Handles input
	fmt.Println(msg.Attempts)
	fmt.Println(msg.ID)
	fmt.Println(msg.NSQDAddress)
	fmt.Println(msg.Timestamp)
	fmt.Println(string(msg.Body))

	return nil
}
