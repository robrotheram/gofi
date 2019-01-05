package Datastore

import (
	"github.com/dgraph-io/badger"
	"github.com/robrotheram/gofi/settings"
	"log"
	"os"
)

type DataStore struct {
	dataFactories map[string]DS
}

type DS interface {
	Close()
	Initialize()
	Search(id string) interface{}
	Edit(obj interface{}) error
	Delete(obj interface{}) error
	GetAll() interface{}
	Save(obj interface{}) error
}

func NewDataStore() *DataStore {
	d := DataStore{}
	d.dataFactories = make(map[string]DS)

	d.RegisterData("GRAPH", graphDataStore{}.New())
	d.RegisterData("USER", userDataStore{}.New())

	return &d
}

func (d *DataStore) Load() {
	for _, ds := range d.dataFactories {
		ds.Initialize()
	}
}

func (d *DataStore) RegisterData(name string, factory DS) {
	if factory == nil {
		log.Panicf("Datastore factory %s does not exist.", name)
	}
	_, registered := d.dataFactories[name]
	if registered {
		log.Println("Datastore factory %s already registered. Ignoring.", name)
	}
	d.dataFactories[name] = factory
}

func (d *DataStore) Close() {
	for _, v := range d.dataFactories {
		v.Close()
	}

}

func (d DataStore) DoesTableExist(table string) bool {
	return d.dataFactories[table] != nil
}

func (d DataStore) Tables(table string) DS {
	return d.dataFactories[table]
}

// Helper function
func createDatastore(ds string) *badger.DB {
	opts := badger.DefaultOptions
	opts.Dir = settings.Settings.DataPath + ds
	opts.ValueDir = settings.Settings.DataPath + ds

	os.MkdirAll(opts.Dir, os.ModePerm)

	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return db
}
