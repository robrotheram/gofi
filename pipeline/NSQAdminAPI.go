package pipeline

import (
	"io/ioutil"
	"log"
	"net/http"
)

var url = "http://192.168.0.125:4151"

func DeleteChannelFromTopic(topic, channel string) {
	log.Println("DELETING CHANNEL!!!")
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
