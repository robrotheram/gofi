package scheduler

import (
	"fmt"
	"injester_test/datastore"
	"injester_test/pipeline"
	"time"
	"errors"
)

/*
Contains classes fo making sure all things works
*/

var Scheduler *scheduler

type scheduler struct {
	processes map[string]pipeline.PipelineJob
	datastore *Datastore.DataStore
}

func CreateScheduler(datastore *Datastore.DataStore) *scheduler {
	s := scheduler{make(map[string]pipeline.PipelineJob), datastore}
	Scheduler = &s
	return Scheduler
}

func (s *scheduler) InitScheduler() {}

func (s *scheduler) StopScheduler() {}

func (s *scheduler) CreateProcess(proc Process) {
	proccess := s.processes[proc.Id]
	if proccess != nil {
		return
	}
	p := pipeline.CreatePiplineJob(proc.Id)
	pipline := *pipeline.PipelineFactories[proc.Type]
	config := pipeline.CreateConfig(proc.Id, proc.Id, proc.Type, proc.Config)
	config.InputTopic = proc.InputTopic
	p.New(pipline())
	p.Register()
	p.SetConfig(config)
	s.processes[proc.Id] = p

	fmt.Println("Process Registered")
}

func (s *scheduler) UpdateProcess(proc Process) {
	if s.processes[proc.Id] == nil {
		return
	}

	p := s.processes[proc.Id]
	config := pipeline.CreateConfig(proc.Id, proc.Id, proc.Type, proc.Config)
	config.InputTopic = proc.InputTopic
	p.SetConfig(config)

	if s.processes[proc.Id].GetStatus().Status == "ACTIVE" {
		go func() {
			s.processes[proc.Id].Stop()
			time.Sleep(5 * time.Second)
			s.processes[proc.Id].Run()
		}()
	}
	fmt.Println("Process Updated")
}

func (s *scheduler) Process() {
	return

	graphs, ok := s.datastore.Tables("GRAPH").GetAll().([]Datastore.Graph)
	if !ok {
		return
	}
	fmt.Println("GRAPH")
	fmt.Println(graphs)

	for _, graph := range graphs {
		for _, v := range graph.Nodes {
			if pipeline.DoesPipelineExist(v.Type) {
				proccess := s.processes[v.ID]
				if proccess != nil {
					proccess.SetConfig(pipeline.CreateConfig(v.ID, v.ID, v.Type, v.Params))
				} else {
					p := pipeline.CreatePiplineJob(v.ID)
					pipline := *pipeline.PipelineFactories[v.Type]
					p.New(pipline())
					p.Register()
					p.SetConfig(pipeline.CreateConfig(v.ID, v.ID, v.Type, v.Params))
					s.processes[v.ID] = p
				}
			} else {
				fmt.Println("Error Process: " + v.Type + " does not exist")
			}

		}
		for _, v := range graph.Connections {
			if s.processes[v.OutputNode] != nil {

				config := s.processes[v.OutputNode].GetConfig()
				config.InputTopic = append(config.InputTopic, v.InputNode)
				s.processes[v.OutputNode].SetConfig(config)

				if s.processes[v.OutputNode].GetStatus().Status == "ACTIVE" {
					go func() {
						s.processes[v.OutputNode].Stop()
						time.Sleep(5 * time.Second)
						s.processes[v.OutputNode].Run()
					}()
				}
			}

		}
	}

	fmt.Println("PROCESS")
	s.Debug()
}

func (s *scheduler) Debug() {
	for k, v := range s.processes {
		fmt.Println("==============================================")
		fmt.Printf("ID: [%s] \n", k)
		pplne := v
		pplne.GetConfig()
		pplne.GetStatus()
		fmt.Println("==============================================")
	}
}

//func (s *scheduler) Run () {
//	s.StopAll()
//	s.Process()
//	s.debug()
//}

func (s *scheduler) StartProcess(id string) error {
	p := s.processes[id]
	if p == nil {
		return errors.New("Pipeline Not found")
	}
	if p.GetStatus().Status == "STOPPED" {
		p.Register()
	}
	p.Run()
	return nil
}

func (s *scheduler) StartAll() {
	for k, v := range s.processes {
		if v != nil {
			s.StatusProcess(k)
		}
	}
}

func (s *scheduler) StatusProcess(id string) (pipeline.PipelineStatus, error) {
	p := s.processes[id]
	if p == nil {
		return pipeline.PipelineStatus{}, errors.New("Pipeline Not found")
	}
	return p.GetStatus(), nil
}
func (s *scheduler) GetProcessConfig(id string) (pipeline.PipeLineJson, error) {
	p := s.processes[id]
	if p == nil {
		return pipeline.PipeLineJson{}, errors.New("Pipeline Not found")
	}
	return p.GetConfig(), nil
}

func (s *scheduler) AllStatusProcess() []pipeline.PipelineStatus {
	status := []pipeline.PipelineStatus{}
	for _, p := range s.processes {
		v := p
		if v != nil {
			status = append(status, v.GetStatus())
		}
	}
	return status
}

func (s *scheduler) StopProcess(id string) error {
	p := s.processes[id]
	if p != nil {
		p.Stop()
		return nil
	}
	return errors.New("Pipeline Not found")
}

func (s *scheduler) DeleteProcess(id string) error {
	p := s.processes[id]
	if p != nil {
		p.Stop()
		delete(s.processes, id)
		pipeline.DeleteTopic(id)
		return nil
	}
	return errors.New("Pipeline Not found")
}

func (s *scheduler) StopAll() {
	fmt.Println("STOPPING ALL PROCESESS")
	for k, p := range s.processes {
		v := p
		fmt.Println("Stopping: " + k)
		v.Stop()
	}
}
