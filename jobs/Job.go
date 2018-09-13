package jobs

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/sirupsen/logrus"
	"injester_test/settings"
	"sync"
)

type Job interface {
	Init(*clientv3.Client, *logrus.Logger, *settings.SettingStore, *chan string, *chan Model)
	Run(context.Context, *sync.WaitGroup)
	GetCount() int
}

type Model interface {
	Type() string
}

type JobJson struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Params string `json:"parmas"`
	Time   string `json:"time,string"`
}

func (j JobJson) New() JobJson {
	return j
}
