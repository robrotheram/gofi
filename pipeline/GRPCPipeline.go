package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/nsqio/go-nsq"
	pb "github.com/robrotheram/gofi/messages"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
	"sync"
)

type GRPCPipeline struct {
	channel     string
	producer    *nsq.Producer
	consumer    []*nsq.Consumer
	settings    PipelineSettings
	inputTopic  []string
	outputTopic string
	stream      pb.Gofi_SendClient

	GrpcAddr      string `json:"address"`
	receivedCount int
	sendCount     int
}

func init() {
	RegisterPipeLine(func() Pipeline {
		return &GRPCPipeline{}
	})
}

func (a *GRPCPipeline) GetType() string {
	return "GRPC"
}

func (s GRPCPipeline) GetCount() (int, int, string) {
	return s.sendCount, s.receivedCount, ""
}

func (p *GRPCPipeline) GetConfig() interface{} {
	return p
}

func (w GRPCPipeline) New() *GRPCPipeline {
	return &w
}

func (p *GRPCPipeline) setConfig(params string) error {
	var anyJson map[string]interface{}
	err := json.Unmarshal([]byte(params), &anyJson)
	if err != nil {
		return err
	}
	p.GrpcAddr = anyJson["address"].(string)
	return nil
}

func (p *GRPCPipeline) Init(settings PipelineSettings, config PipeLineJson) {
	p.settings = settings
	p.inputTopic = config.InputTopic
	p.outputTopic = config.OutputTopic

	p.setConfig(config.Params)
	////u2, err := uuid.NewV4()
	//if err != nil {
	//	fmt.Printf("Something went wrong: %s", err)
	//	return
	//}
	p.channel = p.outputTopic
}

func (p *GRPCPipeline) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	prod, err := nsq.NewProducer(p.settings.NsqAddr, nsq.NewConfig())
	if err != nil {
		fmt.Println(err)
		return
	}
	p.producer = prod
	p.producer.SetLogger(log.New(os.Stderr, "", log.Flags()), nsq.LogLevelWarning)

	conn, err := grpc.Dial(p.GrpcAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("can not connect with server %v", err)
	}

	client := pb.NewGofiClient(conn)
	stream, err := client.Send(ctx)
	if err != nil {
		log.Printf("openn stream error %v", err)
	}
	p.stream = stream

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

	for {
		select {
		case <-ctx.Done():
			p.deleteTopics()
			return
		default:
			resp, err := stream.Recv()
			if err == io.EOF {
				p.deleteTopics()
				return
			}
			if err != nil {
				log.Printf("can not receive %v", err)
				p.deleteTopics()
				return
			}
			p.send(string(resp.GetValue()))
		}
	}
}

func (p *GRPCPipeline) deleteTopics() {
	for _, v := range p.inputTopic {
		if len(v) >= 3 {
			DeleteChannelFromTopic(v, p.channel)
		}
	}
}

func (p *GRPCPipeline) close() {

	p.producer.Stop()

	//TODO Delete Channel need to use HTTP to NSQ See https://github.com/nsqio/nsq/blob/8f6fa1f436a592e609d3168d50f6896486775d03/internal/clusterinfo/data.go#L758
}

func (p *GRPCPipeline) send(message string) {
	p.sendCount++

	//Convert string into a protobuf message
	pb := pb.GofiMessage{Key: p.GetType(), Value: []byte(message)}
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

func (tw *GRPCPipeline) HandleMessage(msg *nsq.Message) error {
	tw.receivedCount++
	//Converts to protobuf message

	var data = &pb.GofiMessage{}
	proto.Unmarshal(msg.Body, data)

	if err := tw.stream.Send(data); err != nil {
		log.Fatalf("can not send %v", err)
	}
	fmt.Printf("Received message from: %s sending to GRPC SERVER \n", data.GetKey())

	return nil
}
