package api

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"injester_test/datastore"
	"injester_test/leaderElection"
	"log"
	"net/http"
	"injester_test/settings"
)

var datastore *Datastore.DataStore
var events *chan string
var router *mux.Router
var handler http.Handler

func SetupServer(store *Datastore.DataStore) {
	fmt.Println("Setting up server")
	datastore = store
	router = mux.NewRouter().StrictSlash(true)
	//BuildUI()
	//leaderElection.Election.Register(buildRouts)
	CreateGraphAPI(router)
	CreateProcessAPI(router)
	CreateUserAPI(router)
	CreateControllerAPI(router)
	router.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("./public/"))))

	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET", "POST", "DELETE", "PUT"},
	})
	handler = c.Handler(router)
	go func() { log.Fatal(http.ListenAndServe(":"+settings.Settings.Port, handler)) }()
}

func LeaderRoute(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !leaderElection.Election.IsLeader() {
			ServeHTTP(w, r)
		} else {
			handler(w, r)
		}
	}
}
