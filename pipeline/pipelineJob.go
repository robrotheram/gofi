package pipeline

import (
	"context"
	"fmt"
	"sync"
)

type pipelineJob struct {
	pipeline Pipeline
	status   PipelineStatus
	metric   int
	config   PipeLineJson
	context  context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type PipelineJob interface {
	New(pipeline Pipeline)
	Register()
	SetConfig(config PipeLineJson)
	//	CheckParams(params string) bool
	Run()
	Stop()
	GetStatus() PipelineStatus
	GetConfig() PipeLineJson
}

type PipelineStatus struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	ReceivedMsg int    `json:"received_msg"`
	SentMsg     int    `json:"sent_msg"`
}

func CreatePiplineJob(name string) PipelineJob {
	p := &pipelineJob{}
	p.status = PipelineStatus{}
	p.status.ID = name
	fmt.Println(name)
	//pipline := *PipelineFactories["SOURCE"]
	//p.pipeline = pipline()
	fmt.Println()
	//p.New(PipelineFactories[name]())
	return p
}

func (p *pipelineJob) New(pipeline Pipeline) {
	if pipeline != nil {
		fmt.Print("PIPELINE!:")
		p.pipeline = pipeline
		p.status.Status = "INACTIVE"
	}
}

func (p *pipelineJob) SetConfig(config PipeLineJson) {
	setting := PipelineSettings{NsqAddr: "127.0.0.1:4150"}
	if config.OutputTopic == "" {
		config.OutputTopic = config.ID
	}
	p.config = config
	p.pipeline.Init(setting, config)
}

func (p *pipelineJob) Register() {
	p.context, p.cancel = context.WithCancel(context.Background())
	p.status.Status = "REGISTERED"
	p.wg = sync.WaitGroup{}
}

func (p *pipelineJob) Run() {
	p.wg.Add(1)
	p.status.Status = "ACTIVE"
	go p.pipeline.Run(p.context, &p.wg)
}

func (p *pipelineJob) Stop() {
	p.status.Status = "STOPPED"
	p.cancel()
	p.wg.Wait()
}

func (p *pipelineJob) GetStatus() PipelineStatus {
	p.status.SentMsg, p.status.ReceivedMsg, p.status.Name = p.pipeline.GetCount()
	return p.status
}

func (p *pipelineJob) GetConfig() PipeLineJson {
	return p.config
}
