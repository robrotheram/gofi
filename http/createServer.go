package http

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"injester_test/datastore"
	"injester_test/scheduler"
	"log"
	"net/http"
)

var datastore *Datastore.DataStore
var events *chan string
var schedular *scheduler.Scheduler

func SetupServer(store *Datastore.DataStore, s *scheduler.Scheduler) {
	fmt.Println("Setting up server")
	//BuildUI()
	router := mux.NewRouter().StrictSlash(true)

	CreateGraphAPI(router)
	CreateProcessAPI(router)
	CreateUserAPI(router)

	datastore = store
	schedular = s

	router.HandleFunc("/data", HomePage).Methods("GET")
	router.HandleFunc("/settings", GetSettings).Methods("GET")

	router.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("./public/"))))

	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET", "POST", "DELETE", "PUT"},
	})

	handler := c.Handler(router)

	log.Fatal(http.ListenAndServe(":8000", handler))
}
