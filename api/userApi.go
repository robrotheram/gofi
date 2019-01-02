package api

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"
	"injester_test/datastore"
	"net/http"
)

func GetPeople(w http.ResponseWriter, r *http.Request) {
	users := datastore.Tables("USER")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users.GetAll())
}

func GetPerson(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	users := datastore.Tables("USER")
	user := users.Search(params["id"])
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
}

func CreatePerson(w http.ResponseWriter, r *http.Request) {
	var user Datastore.User
	_ = json.NewDecoder(r.Body).Decode(&user)
	users := datastore.Tables("USER")
	user.Id = uuid.NewV4().String()
	users.Save(user)
	json.NewEncoder(w).Encode(user)
}

func EditPerson(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	users := datastore.Tables("USER")
	user := users.Search(params["id"])
	var usr Datastore.User
	_ = json.NewDecoder(r.Body).Decode(&usr)
	user = usr
	users.Save(user)
	json.NewEncoder(w).Encode(user)
}

func DeletePerson(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	users := datastore.Tables("USER")
	user := users.Search(params["id"])
	w.Header().Set("Content-Type", "application/json")
	users.Delete(user)
	json.NewEncoder(w).Encode(user)
}

func CreateUserAPI(router *mux.Router) {
	router.HandleFunc("/people", GetPeople).Methods("GET")
	router.HandleFunc("/people/{id}", GetPerson).Methods("GET")
	router.HandleFunc("/people", CreatePerson).Methods("PUT")
	router.HandleFunc("/people/{id}", EditPerson).Methods("POST")
	router.HandleFunc("/people/{id}", DeletePerson).Methods("DELETE")
}
