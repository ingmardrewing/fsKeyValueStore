package main

import (
	"errors"
	"log"
	"os"
	"path"

	"github.com/ingmardrewing/fs"
	scribble "github.com/nanobox-io/golang-scribble"
)

var fsStore persistence

// Creates a filePersistence struct and
// a directory for it to use and
// store the js-files in.
// if no path is given it creates
// a dir with the name "file-db"
// right next to the binary
func Initialize(dirpath ...string) {
	dbDir := path.Join(fs.Pwd(), "file-db")
	if len(dirpath) == 1 {
		dbDir = dirpath[0]
	}
	if !pathExists(dbDir) {
		err := fs.CreateDir(dbDir)
		if err != nil {
			log.Fatal(err)
		}
	}
	fdb, err := newFilePersistence(dbDir)
	if err != nil {
		log.Fatal(err)
	}
	fsStore = fdb
}

// Stores a value under the given key
// if the key / value pair didn't already exist
// as a persisted entity, it will be created
func (fp *filePersistence) CreateIfNonExistentElseUpdate(key, value string) error {
	return fsStore.createIfNonExistentElseUpdate(key, value)
}

// stores a new key / value pair
func Create(key, value string) error {
	return fsStore.create(key, value)
}

// updates an existing key / value pair
func Update(key, value string) error {
	return fsStore.update(key, value)
}

// Reads the value for the given key
// returns an error when it can't find
// the given key
func Read(key string) (string, error) {
	return fsStore.read(key)
}

// Deletes the key and the associated
// value
func Delete(key string) error {
	return fsStore.remove(key)
}

type persistence interface {
	createIfNonExistentElseUpdate(key, value string) error
	create(key string, value string) error
	update(key string, value string) error
	remove(key string) error
	read(key string) (string, error)
}

func newFilePersistence(dir string) (persistence, error) {
	if !pathExists(dir) {
		return nil, errors.New("Given dir for scribble db doesn't exist")
	}

	db, err := scribble.New(dir, nil)
	if err != nil {
		return nil, err
	}

	fp := new(filePersistence)
	fp.dir = dir
	fp.db = db
	return fp, nil
}

type record struct {
	Value string
}

type filePersistence struct {
	dir string
	db  *scribble.Driver
}

func (fp *filePersistence) createIfNonExistentElseUpdate(key, value string) error {
	if fp.exists(key) {
		if err := fp.update(key, value); err != nil {
			return err
		}
		return nil
	}
	if err := fp.create(key, value); err != nil {
		return err
	}
	return nil
}

func (fp *filePersistence) create(key, value string) error {
	if fp.exists(key) {
		return errors.New("Already exists")
	}
	if key == "" {
		return errors.New("Empty string given as key")
	}
	if err := fp.db.Write("record", key, record{Value: value}); err != nil {
		return err
	}
	return nil
}

func (fp *filePersistence) read(key string) (string, error) {
	if key == "" {
		return "", errors.New("Emtpy string given as key")
	}
	r := record{}
	if err := fp.db.Read("record", key, &r); err != nil {
		return "", err
	}
	return r.Value, nil
}

func (fp *filePersistence) update(key, value string) error {
	if !fp.exists(key) {
		return errors.New("Can't update, key doesn't exist")
	}
	if key == "" {
		return errors.New("Empty string given as key")
	}
	if err := fp.db.Write("record", key, record{Value: value}); err != nil {
		return err
	}
	return nil
}

func (fp *filePersistence) remove(key string) error {
	if !fp.exists(key) {
		return errors.New("Can't delete, key doesn't exist")
	}
	if key == "" {
		return errors.New("Empty string given as key")
	}
	if err := fp.db.Delete("record", key); err != nil {
		return err
	}
	return nil
}

func (fp *filePersistence) exists(key string) bool {
	_, err := fp.read(key)
	if err != nil {
		return false
	}
	return true
}

func pathExists(pth string) bool {
	_, err := os.Stat(pth)
	if err == nil {
		return true
	}
	return false
}
