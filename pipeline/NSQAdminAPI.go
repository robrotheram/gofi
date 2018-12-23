package pipeline

import (
	"io/ioutil"
	"log"
	"net/http"
	"injester_test/settings"
)



func DeleteChannelFromTopic(topic, channel string) {
	log.Println("DELETING CHANNEL!!!")
	url := "http://"+settings.Settings.NSQ+":4151"
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
	url := "http://"+settings.Settings.NSQ+":4151"
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
