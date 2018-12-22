package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"injester_test/datastore"
	"injester_test/pipeline"
	"injester_test/settings"
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

type controller struct {
	Worker  string  `json:"worker"`
	Ip      string  `json:"ip"`
	Process Process `json:"process"`
}

var Controller = make(map[string]*controller)

func ControllerProcess() {
	graphs, ok := Scheduler.datastore.Tables("GRAPH").GetAll().([]Datastore.Graph)
	if !ok {
		return
	}
	old := Controller
	for _, graph := range graphs {
		ProccessGraph(graph)
	}
	for _, v := range Controller {
		if v.Worker == "" {
			worker := workoutthebestnode()
			v.Worker = worker.Worker
			v.Ip = worker.Ip
			Health[worker.Worker].NumberPipelines++
			SendToNode(worker.Ip, "PUT", v.Process)
		} else {
			if !old[v.Process.Id].Process.compear(v.Process) {
				SendToNode(v.Ip, "POST", v.Process)
			}
		}
	}

	fmt.Println("Controller Done")
	for _, v := range Controller {
		fmt.Println(v)
	}
}

func Rebalance() {
	for _, v := range Controller {
		if Health[v.Worker] != nil {
			if Health[v.Worker].Status != "ACTIVE" {
				//only do this if the process is not already running
				SendDeleteToNode(v.Ip, v.Process)
				Health[v.Worker].NumberPipelines--
				worker := workoutthebestnode()
				v.Worker = worker.Worker
				v.Ip = worker.Ip
				Health[worker.Worker].NumberPipelines++
				SendToNode(worker.Ip, "PUT", v.Process)
			}
		} else {
			//if the healh reports nill then the node has died and the process will need to be registered to a new node
			worker := workoutthebestnode()
			v.Worker = worker.Worker
			v.Ip = worker.Ip
			Health[worker.Worker].NumberPipelines++
			SendToNode(worker.Ip, "PUT", v.Process)
		}
	}
}

func DeleteProcessConnection(connection Datastore.Connection) {
	for i, v := range Controller[connection.OutputNode].Process.InputTopic {
		if v == connection.InputNode {
			Controller[connection.OutputNode].Process.InputTopic = append(
				Controller[connection.OutputNode].Process.InputTopic[:i],
				Controller[connection.OutputNode].Process.InputTopic[i+1:]...)
			break
		}
	}
	SendToNode(Controller[connection.OutputNode].Ip, "POST", Controller[connection.OutputNode].Process)
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func ProccessGraph(graph Datastore.Graph) {
	for _, v := range graph.Nodes {
		if !pipeline.DoesPipelineExist(v.Type) {
			fmt.Println("Error Process: " + v.Type + " does not exist")
			break
		}
		if Controller[v.ID] == nil {
			p := Process{v.ID, v.Type, v.Params, []string{}, []string{}}
			Controller[v.ID] = &controller{"", "", p}
		}
	}
	for _, v := range graph.Connections {
		if Controller[v.OutputNode] != nil {
			if !Contains(Controller[v.OutputNode].Process.InputTopic, v.InputNode) {
				Controller[v.OutputNode].Process.InputTopic = append(Controller[v.OutputNode].Process.InputTopic, v.InputNode)
			}
		}
	}
}

func workoutthebestnode() *Metrics {
	metric := Health[settings.Settings.Hostname]
	for _, v := range GetHealth() {
		if (v.NumberPipelines < metric.NumberPipelines) && v.Status != UNHEALTHY {
			metric = v
		}
	}
	return metric
}

func StatusProcess(procID string) (pipeline.PipelineStatus, error) {
	if Controller[procID] == nil {
		return pipeline.PipelineStatus{}, errors.New("Pipeline Not found")
	}
	url := "http://" + Controller[procID].Ip + ":8000/process/" + Controller[procID].Process.Id + "/status"
	response, err := http.Get(url)
	if err != nil {
		return pipeline.PipelineStatus{}, err
	}

	var status pipeline.PipelineStatus
	err = json.NewDecoder(response.Body).Decode(&status)
	defer response.Body.Close()
	if err != nil {
		return pipeline.PipelineStatus{}, err
	}
	return status, nil
}

func StartProcess(procID string) error {
	if Controller[procID] == nil {
		return errors.New("Pipeline Not found")
	}
	fmt.Println("Sending START Proc to node: " + Controller[procID].Ip)
	url := "http://" + Controller[procID].Ip + ":8000/process/" + Controller[procID].Process.Id + "/start"
	response, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
	} else {
		body, _ := ioutil.ReadAll(response.Body)
		bodyString := string(body)
		fmt.Println(bodyString)
		defer response.Body.Close()
	}

	return nil
}

func StopProcess(procID string) error {
	if Controller[procID] == nil {
		return errors.New("Pipeline Not found")
	}
	fmt.Println("Sending STOP Proc to node: " + Controller[procID].Ip)
	url := "http://" + Controller[procID].Ip + ":8000/process/" + Controller[procID].Process.Id + "/stop"
	response, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
	} else {
		body, _ := ioutil.ReadAll(response.Body)
		bodyString := string(body)
		fmt.Println(bodyString)
		defer response.Body.Close()
	}
	return nil
}

func SendDeleteToNode(ip string, proc Process) {
	fmt.Println("Sending Proc to node: " + ip)
	url := "http://" + ip + ":8000/process/" + proc.Id
	b, _ := json.Marshal(proc)
	req, err := http.NewRequest("DELETE", url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(body)
		fmt.Println(bodyString)
		defer resp.Body.Close()
	}
}

func SendToNode(ip string, method string, proc Process) {

	fmt.Println("Sending Proc to node: " + ip)
	url := "http://" + ip + ":8000/process"
	b, _ := json.Marshal(proc)
	req, err := http.NewRequest(method, url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(body)
		fmt.Println(bodyString)
		defer resp.Body.Close()
	}
}
