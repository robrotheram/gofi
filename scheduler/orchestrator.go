package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/robrotheram/gofi/datastore"
	"github.com/robrotheram/gofi/pipeline"
	"github.com/robrotheram/gofi/settings"
	"io/ioutil"
	"net/http"
)

type Process struct {
	Id          string   `json:"id"`
	Type        string   `json:"type"`
	Config      string   `json:"config"`
	InputTopic  []string `json:"inputs"`
	OutputTopic []string `json:"outputs"`
}

func (p *Process) compear(p2 Process) bool {
	if p.Id != p2.Id {
		return false
	}
	if p.Type != p2.Type {
		return false
	}
	if p.Config != p2.Config {
		return false
	}
	if len(p.InputTopic) != len(p2.InputTopic) {
		return false
	}
	if len(p.OutputTopic) != len(p2.OutputTopic) {
		return false
	}
	return true
}

type proccessor struct {
	Worker  string  `json:"worker"`
	Ip      string  `json:"ip"`
	Process Process `json:"process"`
}

//var Controller = make(map[string]*controller)

func copy(originalMap map[string]*proccessor) map[string]*proccessor {
	newMap := make(map[string]*proccessor)
	for key, value := range originalMap {
		newMap[key] = value
	}
	return newMap
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

var Orchestrator = NewOrchestator()

type orchestrator struct {
	processes map[string]*proccessor
}

func NewOrchestator() *orchestrator {
	o := orchestrator{
		make(map[string]*proccessor)}
	return &o
}

func (o *orchestrator) AddNode(node Datastore.Node) {
	if !pipeline.DoesPipelineExist(node.Type) {
		fmt.Println("Error Process: " + node.Type + " does not exist")
		return
	}
	if !pipeline.CheckParmas(node.Type, node.Params) {
		fmt.Println("Error Process: " + node.Type + " does not exist")
		return
	}

	if o.processes[node.ID] != nil {
		o.UpdateNode(node)
		return
	}
	if o.processes[node.ID] == nil {
		p := Process{node.ID, node.Type, node.Params, []string{}, []string{}}
		worker := o.findWorker()
		o.processes[node.ID] = &proccessor{worker.Worker, worker.Ip, p}
		o.createProcess(worker.Ip, p)
	}
}

func (o *orchestrator) UpdateNode(node Datastore.Node) {
	if o.processes[node.ID] == nil {
		return
	}
	p := o.processes[node.ID]
	p.Process.Config = node.Params

	if p.Worker == "" {
		worker := o.findWorker()
		o.processes[node.ID].Worker = worker.Worker
		o.processes[node.ID].Ip = worker.Ip
	}
	o.updateProcess(p.Ip, p.Process)
}

func (o *orchestrator) DeleteNode(node Datastore.Node) {
	if o.processes[node.ID] == nil {
		return
	}
	delete(o.processes, node.ID)
	o.deleteProcess(o.processes[node.ID].Ip, o.processes[node.ID].Process)
}

func (o *orchestrator) AddConnection(connection Datastore.Connection) {
	if o.processes[connection.OutputNode] == nil {
		return
	}
	p := o.processes[connection.OutputNode]
	if !Contains(p.Process.InputTopic, connection.InputNode) {
		p.Process.InputTopic = append(p.Process.InputTopic, connection.InputNode)
		o.updateProcess(p.Ip, p.Process)
	}
}

func (o *orchestrator) DeleteConnection(connection Datastore.Connection) {
	if o.processes[connection.OutputNode] == nil {
		return
	}
	p := o.processes[connection.OutputNode]
	if Contains(p.Process.InputTopic, connection.InputNode) {
		for i, item := range p.Process.InputTopic {
			if item == connection.InputNode {
				p.Process.InputTopic = append(p.Process.InputTopic[:i], p.Process.InputTopic[i+1:]...)
			}
		}
		o.updateProcess(p.Ip, p.Process)
	}
}

func (o *orchestrator) ProcessGraph(graph Datastore.Graph) {
	for _, v := range graph.Nodes {
		o.AddNode(v)
	}
	for _, v := range graph.Connections {
		o.AddConnection(v)
	}
}

func (o *orchestrator) Size() int {
	return len(o.processes)
}

func (o *orchestrator) Load() {
	graphs, ok := Scheduler.datastore.Tables("GRAPH").GetAll().([]Datastore.Graph)
	if !ok {
		return
	}
	for _, graph := range graphs {
		o.ProcessGraph(graph)
	}
}

func (o *orchestrator) GetProcessors() map[string]*proccessor {
	return o.processes
}

//When we have a new worker node or if a worker node dies we need to rebalance the non running proccesses across the alive nodes
func (o *orchestrator) Rebalance() {
	for _, v := range o.processes {
		if Health[v.Worker] != nil {
			if Health[v.Worker].Status != "ACTIVE" {
				//only do this if the process is not already running
				o.deleteProcess(v.Ip, v.Process)
				Health[v.Worker].NumberPipelines--
				worker := o.findWorker()
				v.Worker = worker.Worker
				v.Ip = worker.Ip
				Health[worker.Worker].NumberPipelines++
				o.createProcess(worker.Ip, v.Process)
			}
		} else {
			//if the healh reports nill then the node has died and the process will need to be registered to a new node
			worker := o.findWorker()
			v.Worker = worker.Worker
			v.Ip = worker.Ip
			Health[worker.Worker].NumberPipelines++
			o.createProcess(worker.Ip, v.Process)
		}
	}
}

//Search through the list of reported workers and find one that is currently healthy and has the fewest number of processes
func (o *orchestrator) findWorker() *Metrics {
	metric := Health[settings.Settings.Hostname]
	for _, v := range GetHealth() {
		if (v.NumberPipelines < metric.NumberPipelines) && v.Status != UNHEALTHY {
			metric = v
		}
	}
	return metric
}

//Get the processor status by looking up which worker currently is running it and get the status from that node
func (o *orchestrator) StatusProcess(procID string) (pipeline.PipelineStatus, error) {
	if o.processes[procID] == nil {
		return pipeline.PipelineStatus{}, errors.New("Pipeline Not found")
	}

	b, err := o.sendCommand(o.processes[procID].Ip, "process/"+o.processes[procID].Process.Id+"/status", "GET", nil)
	if err != nil {
		return pipeline.PipelineStatus{}, err
	}
	var status pipeline.PipelineStatus
	err = json.Unmarshal(b, &status)
	return status, nil
}

//Get the processor status by looking up which worker currently is running it and call start
func (o *orchestrator) StartProcess(procID string) ([]byte, error) {
	if o.processes[procID] == nil {
		return nil, errors.New("Pipeline Not found")
	}
	fmt.Println("Sending START Proc to node: " + o.processes[procID].Ip)
	return o.sendCommand(o.processes[procID].Ip, "process/"+o.processes[procID].Process.Id+"/start", "GET", nil)
}

//Get the processor status by looking up which worker currently is running it and call stop
func (o *orchestrator) StopProcess(procID string) ([]byte, error) {
	if o.processes[procID] == nil {
		return nil, errors.New("Pipeline Not found")
	}
	fmt.Println("Sending STOP Proc to node: " + o.processes[procID].Ip)
	return o.sendCommand(o.processes[procID].Ip, "process/"+o.processes[procID].Process.Id+"/stop", "GET", nil)
}

func (o *orchestrator) createProcess(ip string, proc Process) {
	o.sendCommand(ip, "process", "PUT", proc)
}

func (o *orchestrator) updateProcess(ip string, proc Process) {
	o.sendCommand(ip, "process", "POST", proc)
}

func (o *orchestrator) deleteProcess(ip string, proc Process) {
	o.sendCommand(ip, "process/"+proc.Id, "DELETE", proc)
}

func (o *orchestrator) sendCommand(ip string, command string, method string, data interface{}) ([]byte, error) {
	url := fmt.Sprintf("%s://%s:%s/%s", settings.Settings.Transport, ip, settings.Settings.Port, command)

	if method == "GET" {
		response, err := http.Get(url)
		if err != nil {
			return nil, err
		} else {
			body, err := ioutil.ReadAll(response.Body)
			defer response.Body.Close()
			return body, err
		}
	}
	b, _ := json.Marshal(data)
	req, err := http.NewRequest(method, url, bytes.NewBuffer(b))

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	} else {
		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	}
}
