package pipeline

import (
	"github.com/nsqio/nsq/nsqd"
	"github.com/robrotheram/gofi/leaderElection"
	"io/ioutil"
	"log"
	"net/http"
)

func DeleteChannelFromTopic(topic, channel string) {
	log.Println("DELETING CHANNEL!!!")
	url := "http://" + leaderElection.Election.LeaderIP() + ":4151"
	resp, err := http.Post(url+"/channel/delete?topic="+topic+"&channel="+channel, "application/json", nil)
	if err != nil {
		log.Fatalln(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(body))
}

func DeleteTopic(topic string) {
	log.Println("DELETING CHANNEL!!!")
	url := "http://" + leaderElection.Election.LeaderIP() + ":4151"
	resp, err := http.Post(url+"/topic/delete?topic="+topic, "application/json", nil)
	if err != nil {
		log.Println(err)
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
	}
	log.Println(string(body))
}

func StartNSQ() {
	n := nsqd.New(nsqd.NewOptions())
	n.Main()
}
