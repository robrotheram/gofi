package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"reflect"
	"sync"
)

type Pipeline interface {
	Init(settings PipelineSettings, config PipeLineJson)
	Run(context.Context, *sync.WaitGroup)
	HandleMessage(msg *nsq.Message) error
	GetCount() (int, int, string)
	GetType() string
}

type PipelineSettings struct {
	NsqAddr string
}

type PipeLineJson struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Params      string   `json:"parmas"`
	Time        string   `json:"time"`
	InputTopic  []string `json:"input_topic"`
	OutputTopic string   `json:"output_topic"`
}

func CreateConfig(id string, name string, typ string, params string) PipeLineJson {
	p := PipeLineJson{}
	p.ID = id
	p.Name = name
	p.Type = typ
	p.Params = params

	fmt.Println(p)
	return p
}

func GetListOfParmas(config interface{}) []string {
	js, _ := json.Marshal(config)
	var result map[string]interface{}
	json.Unmarshal(js, &result)
	keys := reflect.ValueOf(result).MapKeys()
	var list2 []string
	for _, x := range keys {
		list2 = append(list2, x.String()) // note the = instead of :=
	}
	return list2
}

type PipeLineParams struct {
	Type   string   `json:"type"`
	Params []string `json:"params"`
}

var PipelineFactories = make(map[string]*func() Pipeline)
var PipelineParamsList = []PipeLineParams{}

func RegisterPipeLine(factory func() Pipeline) {
	if factory == nil {
		log.Panicf("Datastore factory does not exist.")
	}
	name := factory().GetType()
	_, registered := PipelineFactories[name]
	if registered {
		log.Println("Datastore factory %s already registered. Ignoring.", name)
	}

	PipelineFactories[factory().GetType()] = &factory

	PipelineParamsList = append(PipelineParamsList, PipeLineParams{
		name,
		GetListOfParmas(factory())})
}

func Getp(name string) {
	_, p := PipelineFactories[name]
	if p {

	}

}

func DoesPipelineExist(key string) bool {
	if PipelineFactories[key] != nil {
		return true
	}
	return false
}

func CheckParmas(key string, params string) bool {

	var anyJson map[string]interface{}
	json.Unmarshal([]byte(params), &anyJson)

	for _, v := range PipelineParamsList {
		if v.Type == key {
			found := true
			for _, val := range v.Params {
				if anyJson[val] == nil {
					found = false
				}
			}
			return found
		}
	}
	return false
}
