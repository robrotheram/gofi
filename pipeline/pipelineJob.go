package pipeline

import (
	"context"
	"fmt"
	"github.com/robrotheram/gofi/leaderElection"
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
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Status      string   `json:"status"`
	Inputs      []string `json:"inputs"`
	Output      string   `json:"outputs"`
	ReceivedMsg int      `json:"received_msg"`
	SentMsg     int      `json:"sent_msg"`
	Params      string   `json:"params"`
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
	setting := PipelineSettings{NsqAddr: leaderElection.Election.LeaderIP() + ":4150"}
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
	p.context, p.cancel = context.WithCancel(context.Background())
	p.status.Status = "ACTIVE"
	go p.pipeline.Run(p.context, &p.wg)
}

func (p *pipelineJob) Stop() {
	p.status.Status = "STOPPED"
	fmt.Println("CALLING STOPS")
	p.cancel()
	p.wg.Wait()
}

func (p *pipelineJob) GetStatus() PipelineStatus {
	p.status.SentMsg, p.status.ReceivedMsg, p.status.Name = p.pipeline.GetCount()
	p.status.Inputs = p.config.InputTopic
	p.status.Output = p.config.OutputTopic
	p.status.Params = p.config.Params
	return p.status
}

func (p *pipelineJob) GetConfig() PipeLineJson {
	return p.config
}
