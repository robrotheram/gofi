package api

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/robrotheram/gofi/scheduler"
	"net/http"
)

func putHealth(w http.ResponseWriter, r *http.Request) {
	var metric scheduler.Metrics
	_ = json.NewDecoder(r.Body).Decode(&metric)

	isNew := scheduler.Health[metric.Worker] == nil
	scheduler.Health[metric.Worker] = &metric
	if isNew {
		fmt.Println("NEW NODE CONNECTED!!!")
		scheduler.Orchestrator.Rebalance()
	}
}

func getHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scheduler.GetHealth())
}

func getControllers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scheduler.Scheduler)
}

func getProcess(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	id := params["id"]
	status, _ := scheduler.Scheduler.StatusProcess(id)
	json.NewEncoder(w).Encode(status)
}

func createProcess(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var proc = scheduler.Process{}
	err := json.NewDecoder(r.Body).Decode(&proc)
	if err != nil {
		return
	}
	scheduler.Scheduler.CreateProcess(proc)
	json.NewEncoder(w).Encode(scheduler.Scheduler)
}

func updateProcess(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var proc = scheduler.Process{}
	err := json.NewDecoder(r.Body).Decode(&proc)
	if err != nil {
		return
	}
	scheduler.Scheduler.UpdateProcess(proc)
	json.NewEncoder(w).Encode(scheduler.Scheduler)
}

func deteleProcess(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	id := params["id"]
	err := scheduler.Scheduler.DeleteProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Process DELETED"))
}

func stopProcess(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	err := scheduler.Scheduler.StopProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Process Stopped"))
}

func startProcess(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	err := scheduler.Scheduler.StartProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Process Started"))
}

func statusProcess(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	status, err := scheduler.Scheduler.StatusProcess(id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func CreateControllerAPI(router *mux.Router) {

	router.HandleFunc("/controller/health", LeaderRoute(putHealth)).Methods("POST")
	router.HandleFunc("/controller/health", LeaderRoute(getHealth)).Methods("GET")
	router.HandleFunc("/controller", LeaderRoute(getControllers)).Methods("GET")

	router.HandleFunc("/process", createProcess).Methods("PUT")
	router.HandleFunc("/process", updateProcess).Methods("POST")
	router.HandleFunc("/process/{id}", getProcess).Methods("GET")
	router.HandleFunc("/process/{id}", deteleProcess).Methods("DELETE")

	router.HandleFunc("/process/{id}/stop", stopProcess).Methods("GET")
	router.HandleFunc("/process/{id}/start", startProcess).Methods("GET")
	router.HandleFunc("/process/{id}/status", statusProcess).Methods("GET")
}
