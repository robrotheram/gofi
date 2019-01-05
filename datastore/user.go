package Datastore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger"
)

type User struct {
	Id       string   `json:"id"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	Email    string   `json:"email"`
	Graphs   []string `json:"graphs"`
}

type userDataStore struct {
	db    *badger.DB
	Users []User
}

func (u userDataStore) New() *userDataStore {
	u = userDataStore{}
	return &u
}

func (u *userDataStore) Initialize() {
	u.db = createDatastore("users")
	//u.Load()
}

func (u *User) serialize() []byte {
	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	if err := e.Encode(u); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (u *User) deserialize(b []byte) error {
	dCache := bytes.NewBuffer(b)
	d := gob.NewDecoder(dCache)
	if err := d.Decode(u); err != nil {
		return err
	}
	return nil
}

func (uDs *userDataStore) Search(id string) interface{} {
	for _, item := range uDs.Users {
		if item.Id == id {
			return item
		}
	}
	return Graph{}
}

func (uDs *userDataStore) Edit(u interface{}) error {
	original, ok := u.(Graph)
	if ok {
		err := uDs.db.Update(func(tx *badger.Txn) error {
			fmt.Println(original.Id)
			return tx.Set([]byte(original.Id), original.serialize())
		})
		uDs.load()
		fmt.Println(err)
		return err
	}
	fmt.Println("NO ERROR")
	return nil
}

func (uDs *userDataStore) load() error {
	var user = []User{}
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
			u := User{}
			error := u.deserialize(data)
			if error != nil {
				return error
			}
			user = append(user, u)
			return nil
		}
		return nil
	})
	uDs.Users = user
	return err
}

func (uDs *userDataStore) GetUser(id string) (User, error) {

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
		return User{}, err
	}
	u := User{}
	u.deserialize(valCopy)
	return u, nil
}

func (uDs *userDataStore) Save(u interface{}) error {
	original, ok := u.(User)
	if ok {
		err := uDs.db.Update(func(tx *badger.Txn) error {
			return tx.Set([]byte(original.Id), original.serialize())
		})
		uDs.load()
		return err
	}
	return nil
}

func (uDs *userDataStore) Delete(u interface{}) error {
	original, ok := u.(User)
	if ok {
		err := uDs.db.Update(func(tx *badger.Txn) error {
			return tx.Delete([]byte(original.Id))
		})
		uDs.load()
		return err
	}
	return nil
}

func (uDs *userDataStore) GetAll() interface{} {

	return uDs.Users
}

func (uDs *userDataStore) Close() {
	uDs.db.Close()
}
