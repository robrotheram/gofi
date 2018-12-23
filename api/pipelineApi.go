package api

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"injester_test/pipeline"
	"injester_test/scheduler"
	"net/http"
)

func StartProccess(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	_, err := scheduler.Orchestrator.StartProcess(id)
	//scheduler.Scheduler.StartProcess(id)
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
	status, err := scheduler.Orchestrator.StatusProcess(id)
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
	config, err := scheduler.Scheduler.GetProcessConfig(id)
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
	if len(scheduler.Scheduler.AllStatusProcess()) == 0 {
		scheduler.Scheduler.Process()
	}

	json.NewEncoder(w).Encode(scheduler.Scheduler.AllStatusProcess())

}

func StopProccess(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	_, err := scheduler.Orchestrator.StopProcess(id)
	//scheduler.Scheduler.StopProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Process Stopped"))
}

func StartAll(w http.ResponseWriter, r *http.Request) {
	scheduler.Scheduler.StartAll()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("All Process Started"))
}

func StopAll(w http.ResponseWriter, r *http.Request) {
	scheduler.Scheduler.StopAll()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("All Process Stopped"))
}

func GetParams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pipeline.PipelineParamsList)
}

func CreateProcessAPI(router *mux.Router) {
	fmt.Println("Creating Pipeline Router")

	router.HandleFunc("/pipeline", (StatusAll)).Methods("GET")
	router.HandleFunc("/pipeline/params", LeaderRoute(GetParams)).Methods("GET")
	router.HandleFunc("/pipeline/start", LeaderRoute(StartAll)).Methods("GET")
	router.HandleFunc("/pipeline/stop", LeaderRoute(StopAll)).Methods("GET")
	router.HandleFunc("/pipeline/{id}/status", LeaderRoute(Status)).Methods("GET")
	router.HandleFunc("/pipeline/{id}/config", LeaderRoute(Config)).Methods("GET")
	router.HandleFunc("/pipeline/{id}/stop", LeaderRoute(StopProccess)).Methods("GET")
	router.HandleFunc("/pipeline/{id}/start", LeaderRoute(StartProccess)).Methods("GET")

}
