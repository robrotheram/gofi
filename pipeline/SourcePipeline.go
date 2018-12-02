package pipeline

import (
	"context"
	"fmt"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"log"
	"os"
	"sync"
	"time"
)

type SourcePipeline struct {
	channel     string
	producer    *nsq.Producer
	consumer    *nsq.Consumer
	settings    PipelineSettings
	inputTopic  string
	outputTopic string

	receivedCount int
	sendCount     int

	name string
}

func init() {
	RegisterPipeLine(func() Pipeline {
		return &SourcePipeline{}
	})
}

func (a *SourcePipeline) GetType() string {
	return "SOURCE"
}

func (s SourcePipeline) GetCount() (int, int, string) {
	return s.sendCount, s.receivedCount, s.name
}

func (p *SourcePipeline) GetConfig() interface{} {
	return p
}

func (w SourcePipeline) New() *SourcePipeline {
	return &w
}

func (p *SourcePipeline) Init(settings PipelineSettings, config PipeLineJson) {
	p.settings = settings
	p.name = config.Name
	//p.inputTopic = config.InputTopic
	p.outputTopic = config.OutputTopic

	u2, err := uuid.NewV4()
	if err != nil {
		fmt.Printf("Something went wrong: %s", err)
		return
	}
	p.channel = u2.String()
}

func (p *SourcePipeline) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	p.producer, _ = nsq.NewProducer(p.settings.NsqAddr, nsq.NewConfig())
	p.producer.SetLogger(log.New(os.Stderr, "", log.Flags()), nsq.LogLevelWarning)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			p.send("Time is now: " + now.String())
		case <-ctx.Done():
			p.close()
			return
		}
	}
}

func (p *SourcePipeline) close() {

	p.producer.Stop()

	//TODO Delete Channel need to use HTTP to NSQ See https://github.com/nsqio/nsq/blob/8f6fa1f436a592e609d3168d50f6896486775d03/internal/clusterinfo/data.go#L758
}

func (p *SourcePipeline) send(message string) {
	p.sendCount++
	p.producer.Publish(p.outputTopic, []byte(message))
}

func (tw *SourcePipeline) HandleMessage(msg *nsq.Message) error {

	// Handles input
	fmt.Println(msg.Attempts)
	fmt.Println(msg.ID)
	fmt.Println(msg.NSQDAddress)
	fmt.Println(msg.Timestamp)
	fmt.Println(string(msg.Body))

	tw.send(string(msg.Body))
	return nil
}