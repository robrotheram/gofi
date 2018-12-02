package http

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"
	"injester_test/datastore"
	"injester_test/pipeline"
	"net/http"
)

func GetGraphs(w http.ResponseWriter, r *http.Request) {
	graph := datastore.Tables("GRAPH")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph.GetAll())
}

// Display a single data
func GetGraph(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	graph := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

func status(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	graph := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	status := []pipeline.PipelineStatus{}
	for _, v := range graph.Nodes {
		state, err := schedular.StatusProcess(v.ID)
		if err == nil {
			status = append(status, state)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// create a new item
func CreateNode(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}

	var node Datastore.Node
	err := json.NewDecoder(r.Body).Decode(&node)
	if err != nil {
		return
	}

	if pipeline.DoesPipelineExist(node.Type) {
		if pipeline.CheckParmas(node.Type, node.Params) {
			node.ID = uuid.Must(uuid.NewV4()).String()
			graph.Nodes = append(graph.Nodes, node)
			fmt.Println(graph)
			err := datastore.Tables("GRAPH").Save(graph)
			if err != nil {
				fmt.Println(err)
			} else {
				schedular.Process()
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(graph)
				return
			}
		}

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Params were incorrect"))
		return

	}

	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("500 - Job does not exit"))

}

// create a new item
func EditNode(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}

	//edit node
	for i, item := range graph.Nodes {
		if item.ID == params["nid"] {
			_ = json.NewDecoder(r.Body).Decode(&item)
			graph.Nodes[i] = item
		}
	}
	fmt.Println(graph)
	datastore.Tables("GRAPH").Save(graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

func DeleteNode(w http.ResponseWriter, r *http.Request) {

	params := mux.Vars(r)
	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}

	//delete node
	for i, item := range graph.Nodes {
		if item.ID == params["nid"] {
			graph.Nodes = append(graph.Nodes[:i], graph.Nodes[i+1:]...)
		}
	}
	err := schedular.DeleteProcess(params["nid"])
	fmt.Println(err)
	for i, item := range graph.Connections {
		if item.OutputNode == params["nid"] || item.InputNode == params["nid"] {
			graph.Connections = append(graph.Connections[:i], graph.Connections[i+1:]...)
		}
	}

	datastore.Tables("GRAPH").Save(graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

// create a new item
func CreateConnection(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}
	var node Datastore.Connection
	_ = json.NewDecoder(r.Body).Decode(&node)
	node.ID = uuid.Must(uuid.NewV4()).String()
	graph.Connections = append(graph.Connections, node)
	err := datastore.Tables("GRAPH").Save(graph)
	if err != nil {
		fmt.Println(err)
	} else {
		schedular.Process()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(graph)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

// create a new item
func EditConnection(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}

	var dconnection Datastore.Connection
	_ = json.NewDecoder(r.Body).Decode(&dconnection)

	//edit node
	for i, item := range graph.Connections {
		if item.ID == params["cid"] {
			graph.Connections[i] = dconnection
		}
	}
	datastore.Tables("GRAPH").Save(graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

func DeleteConnection(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}
	//delete node
	for i, item := range graph.Connections {
		if item.ID == params["cid"] {
			graph.Connections = append(graph.Connections[:i], graph.Connections[i+1:]...)
		}
	}

	datastore.Tables("GRAPH").Save(graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

// create a new item
func CreateGraph(w http.ResponseWriter, r *http.Request) {
	var graph Datastore.Graph
	_ = json.NewDecoder(r.Body).Decode(&graph)
	graph.Id = uuid.Must(uuid.NewV4()).String()
	datastore.Tables("GRAPH").Save(graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

// create a new item
func EditGraph(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}
	_ = json.NewDecoder(r.Body).Decode(&graph)

	datastore.Tables("GRAPH").Save(graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

// create a new item
func DeleteGraph(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	ds := datastore.Tables("GRAPH")
	graph, ok := datastore.Tables("GRAPH").Search(params["id"]).(Datastore.Graph)
	if !ok {
		return
	}
	ds.Delete(graph)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(graph)
}

func GetPipelineParams(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pipeline.PipelineParamsList)

}

func CreateGraphAPI(router *mux.Router) {

	fmt.Println("Creating Graph Router")

	router.HandleFunc("/graph/settings", GetPipelineParams).Methods("GET")
	router.HandleFunc("/graph", GetGraphs).Methods("GET")
	router.HandleFunc("/graph", CreateGraph).Methods("PUT")

	router.HandleFunc("/graph/{id}", GetGraph).Methods("GET")
	router.HandleFunc("/graph/{id}", EditGraph).Methods("POST")
	router.HandleFunc("/graph/{id}", DeleteGraph).Methods("DELETE")

	router.HandleFunc("/graph/{id}/status", status).Methods("GET")

	router.HandleFunc("/graph/{id}/node", CreateNode).Methods("PUT")
	router.HandleFunc("/graph/{id}/connection", CreateConnection).Methods("PUT")

	router.HandleFunc("/graph/{id}/node/{nid}", EditNode).Methods("POST")
	router.HandleFunc("/graph/{id}/connection/{cid}", EditConnection).Methods("POST")

	router.HandleFunc("/graph/{id}/node/{nid}", DeleteNode).Methods("DELETE")
	router.HandleFunc("/graph/{id}/connection/{cid}", DeleteConnection).Methods("DELETE")

}
