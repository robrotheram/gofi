package http

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"injester_test/pipeline"
	"net/http"
)

func StartProccess(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	err := schedular.StartProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Process Started"))

}

func Status(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	status, err := schedular.StatusProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)

}

func Config(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	config, err := schedular.GetProcessConfig(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)

}

func StatusAll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if len(schedular.AllStatusProcess()) == 0 {
		schedular.Process()
	}

	json.NewEncoder(w).Encode(schedular.AllStatusProcess())

}

func StopProccess(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	err := schedular.StopProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Process Stopped"))
}

func StartAll(w http.ResponseWriter, r *http.Request) {
	schedular.StartAll()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("All Process Started"))
}

func StopAll(w http.ResponseWriter, r *http.Request) {
	schedular.StopAll()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("All Process Stopped"))
}

func GetParams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pipeline.PipelineParamsList)
}

func CreateProcessAPI(router *mux.Router) {
	fmt.Println("Creating Pipeline Router")

	router.HandleFunc("/pipeline", StatusAll).Methods("GET")
	router.HandleFunc("/pipeline/params", GetParams).Methods("GET")
	router.HandleFunc("/pipeline/start", StartAll).Methods("GET")
	router.HandleFunc("/pipeline/stop", StopAll).Methods("GET")
	router.HandleFunc("/pipeline/{id}/status", Status).Methods("GET")
	router.HandleFunc("/pipeline/{id}/config", Config).Methods("GET")
	router.HandleFunc("/pipeline/{id}/stop", StopProccess).Methods("GET")
	router.HandleFunc("/pipeline/{id}/start", StartProccess).Methods("GET")

}
