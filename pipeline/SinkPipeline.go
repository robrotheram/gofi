package pipeline

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/nsqio/go-nsq"
	"github.com/robrotheram/gofi/messages"
	"log"
	"os"
	"sync"
)

type SinkPipeline struct {
	channel     string
	producer    *nsq.Producer
	consumer    []*nsq.Consumer
	settings    PipelineSettings
	inputTopic  []string
	outputTopic string

	receivedCount int
	sendCount     int
}

func init() {
	RegisterPipeLine(func() Pipeline {
		return &SinkPipeline{}
	})
}

func (a *SinkPipeline) GetType() string {
	return "SINK"
}

func (s SinkPipeline) GetCount() (int, int, string) {
	return s.sendCount, s.receivedCount, ""
}

func (p *SinkPipeline) GetConfig() interface{} {
	return p
}

func (w SinkPipeline) New() *SinkPipeline {
	return &w
}

func (p *SinkPipeline) Init(settings PipelineSettings, config PipeLineJson) {
	p.settings = settings
	p.inputTopic = config.InputTopic
	p.outputTopic = config.OutputTopic

	////u2, err := uuid.NewV4()
	//if err != nil {
	//	fmt.Printf("Something went wrong: %s", err)
	//	return
	//}
	p.channel = p.outputTopic
}

func (p *SinkPipeline) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for _, v := range p.inputTopic {
		if len(v) >= 3 {
			consumer, _ := nsq.NewConsumer(v, p.channel, nsq.NewConfig())
			consumer.SetLogger(log.New(os.Stderr, "", log.Flags()), nsq.LogLevelWarning)
			consumer.AddHandler(p)
			err := consumer.ConnectToNSQD(p.settings.NsqAddr)

			context.Background()
			p.consumer = append(p.consumer, consumer)
			if err != nil {
				log.Panic("Could not connect")
			}
		}
	}

	<-ctx.Done()
	for _, v := range p.inputTopic {
		if len(v) >= 3 {

			DeleteChannelFromTopic(v, p.channel)
		}
	}
}

func (p *SinkPipeline) close() {

	p.producer.Stop()

	//TODO Delete Channel need to use HTTP to NSQ See https://github.com/nsqio/nsq/blob/8f6fa1f436a592e609d3168d50f6896486775d03/internal/clusterinfo/data.go#L758
}

func (p *SinkPipeline) send(message string) {
	p.sendCount++
	p.producer.Publish(p.outputTopic, []byte(message))

}

func (tw *SinkPipeline) HandleMessage(msg *nsq.Message) error {
	tw.receivedCount++
	//Converts to protobuf message
	var data = &messages.GofiMessage{}
	proto.Unmarshal(msg.Body, data)
	fmt.Printf("Received message from: %s \n Message contains: %s \n", data.GetKey(), string(data.GetValue()))

	// Handles input
	//fmt.Println(msg.Attempts)
	//fmt.Println(msg.ID)
	//fmt.Println(msg.NSQDAddress)
	//fmt.Println(msg.Timestamp)

	//tw.send(string(msg.Body))
	return nil
}
