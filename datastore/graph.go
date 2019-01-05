package Datastore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger"
)

type Meta struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type Node struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Meta   Meta   `json:"meta"`
	Params string `json:"params"`
}

type Connection struct {
	ID         string `json:"nid"`
	InputNode  string `json:"input_node"`
	OutputNode string `json:"output_node"`
}

type Graph struct {
	Id          string       `json:"id"`
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Nodes       []Node       `json:"nodes"`
	Connections []Connection `json:"connections"`
}

type graphDataStore struct {
	db     *badger.DB
	Graphs []Graph
}

func (u graphDataStore) New() *graphDataStore {
	u = graphDataStore{}
	return &u
}

func (u *graphDataStore) Initialize() {
	u.db = createDatastore("graph")
	u.Load()
}

func (u *Graph) serialize() []byte {
	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	if err := e.Encode(u); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (u *Graph) deserialize(b []byte) error {
	dCache := bytes.NewBuffer(b)
	d := gob.NewDecoder(dCache)
	if err := d.Decode(u); err != nil {
		return err
	}
	return nil
}

func (uDs *graphDataStore) save() error {
	for _, element := range uDs.Graphs {
		err := uDs.Edit(element)
		if err != nil {
			return err
		}
	}
	return nil
}

func (uDs *graphDataStore) Load() error {
	var Graphs = []Graph{}
	err := uDs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			data, err := item.Value()
			if err != nil {
				return err
			}
			u := Graph{}
			error := u.deserialize(data)
			if error != nil {
				return error
			}
			Graphs = append(Graphs, u)
		}
		return nil
	})
	//fmt.Println((Graphs))
	uDs.Graphs = Graphs
	return err
}

func (uDs *graphDataStore) Search(id string) interface{} {
	for _, item := range uDs.Graphs {
		if item.Id == id {
			return item
		}
	}
	return Graph{}
}

func (uDs *graphDataStore) SearchNode(g []Node, id string) *Node {
	for _, item := range g {
		if item.ID == id {
			return &item
		}
	}
	return nil
}

func (uDs *graphDataStore) DeleteNode(g []Node, id string) []Node {
	fmt.Println(g)
	for i, item := range g {
		if item.ID == id {
			g = append(g[:i], g[i+1:]...)
		}
	}
	fmt.Println(g)
	return g
}

func (uDs *graphDataStore) EditNode(g []Node, id string, node Node) {
	for i, item := range g {
		if item.ID == id {
			g[i] = node
		}
	}
}

func (uDs *graphDataStore) SearchConnection(g []Connection, id string) Connection {
	for _, item := range g {
		if item.ID == id {
			return item
		}
	}
	return Connection{}
}

func (uDs *graphDataStore) DeleteConnection(g []Connection, id string) []Connection {
	fmt.Println(g)
	for i, item := range g {
		if item.ID == id {
			g = append(g[:i], g[i+1:]...)
		}
	}
	fmt.Println(g)
	return g
}

func (uDs *graphDataStore) EditConnection(g []Connection, id string, node Connection) {
	for i, item := range g {
		if item.ID == id {
			g[i] = node
		}
	}
}

func (uDs *graphDataStore) GetGraph(id string) (Graph, error) {

	var valCopy []byte
	err := uDs.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(id))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return nil
	})
	if err != nil {
		return Graph{}, err
	}
	u := Graph{}
	u.deserialize(valCopy)
	return u, nil
}

func (uDs *graphDataStore) Edit(u interface{}) error {
	original, ok := u.(Graph)
	if ok {
		err := uDs.db.Update(func(tx *badger.Txn) error {
			fmt.Println(original.Id)
			return tx.Set([]byte(original.Id), original.serialize())
		})
		uDs.Load()
		fmt.Println(err)
		return err
	}
	fmt.Println("NO ERROR")
	return nil
}

func (uDs *graphDataStore) Save(u interface{}) error {
	original, ok := u.(Graph)
	if ok {
		err := uDs.db.Update(func(tx *badger.Txn) error {
			fmt.Println(original.Id)
			return tx.Set([]byte(original.Id), original.serialize())
		})
		uDs.Load()
		fmt.Println(err)
		return err
	}
	fmt.Println("NO ERROR")
	return nil
}

func (uDs *graphDataStore) Delete(u interface{}) error {
	original, ok := u.(Graph)
	if ok {
		err := uDs.db.Update(func(tx *badger.Txn) error {
			return tx.Delete([]byte(original.Id))
		})
		uDs.Load()
		return err
	}
	return nil
}

func (uDs *graphDataStore) GetAll() interface{} {

	return uDs.Graphs
}

func (uDs *graphDataStore) Close() {
	uDs.db.Close()
}
