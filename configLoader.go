package main

import (
	"github.com/go-yaml/yaml"
	"io/ioutil"
	"os"
)

func getFromFile() {

	confContent, err := ioutil.ReadFile("conf.yml")
	if err != nil {
		panic(err)
	}
	// expand environment variables
	confContent = []byte(os.ExpandEnv(string(confContent)))
	if err := yaml.Unmarshal(confContent, Settings); err != nil {
		panic(err)
	}

	//fmt.Printf("config: %v\n", string(b))
}

func getFromEnviroment() {
	Settings.SetETCDString(os.Getenv(CONFIG_ETCD))
	Settings.SetNumberOfworkers(os.Getenv(CONFIG_WORKERS))
	Settings.SetLogLevel(os.Getenv(CONFIG_LOG_LEVEL))
	Settings.setLogOutut(os.Getenv(CONFIG_LOG_OUTPUT))
}
