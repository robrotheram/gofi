package jobs

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/sirupsen/logrus"
	"injester_test/settings"
	"log"
	"sync"
)

type Job interface {
	New(JobJson)
	Init(*clientv3.Client, *logrus.Logger, *settings.SettingStore, *chan string, *chan Model)
	Run(context.Context, *sync.WaitGroup)
	GetCount() int
	GetParams() *JobParams
}

type CoreJob interface {
	Init(*clientv3.Client, *logrus.Logger, *settings.SettingStore, *chan string, *chan Model)
	Run(context.Context, *sync.WaitGroup)
	GetCount() int
	GetParams() *JobParams
}

type Model interface {
	Type() string
}

type JobJson struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Params string `json:"parmas"`
	Time   string `json:"time"`
}

type JobParams struct {
	Type   string   `json:"type"`
	Params []string `json:"params"`
}

func (j JobJson) New() JobJson {
	return j
}

var JobFactories = make(map[string]Job)

func RegisterJob(name string, factory Job) {
	if factory == nil {
		log.Panicf("Datastore factory %s does not exist.", name)
	}
	_, registered := JobFactories[name]
	if registered {
		log.Println("Datastore factory %s already registered. Ignoring.", name)
	}
	JobFactories[name] = factory
}

func CreateJob(name string) Job {
	return JobFactories[name]
}

func JobsSettings() []JobParams {
	params := []JobParams{}
	for _, v := range JobFactories {
		params = append(params, *v.GetParams())
	}
	return params
}
