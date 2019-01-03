package pipeline

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/nsqio/go-nsq"
	"github.com/robrotheram/gofi/messages"
	"github.com/satori/go.uuid"
	"log"
	"math/rand"
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

	u2 := uuid.NewV4()

	p.channel = u2.String()
}

func (p *SourcePipeline) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("STARTING SPURCE")
	prod, err := nsq.NewProducer(p.settings.NsqAddr, nsq.NewConfig())
	if err != nil {
		fmt.Println(err)
		return
	}
	p.producer = prod
	p.producer.SetLogger(log.New(os.Stderr, "", log.Flags()), nsq.LogLevelWarning)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			rand.Seed(now.UnixNano())
			p.send(fmt.Sprintf("%d", rand.Intn(1000)))
		case <-ctx.Done():
			p.close()
			fmt.Println("STOPPING!!!! SPURCE")
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

	//Convert string into a protobuf message
	pb := messages.GofiMessage{Key: p.GetType(), Value: []byte(message)}
	data, err := proto.Marshal(&pb)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = p.producer.Publish(p.outputTopic, data)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Sending message: " + message)

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
