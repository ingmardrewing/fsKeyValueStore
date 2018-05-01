package main

import (
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/ingmardrewing/fs"
)

func TestMain(m *testing.M) {
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func tearDown() {
	dir := path.Join(getTestFileDirPath(), "testResources/db")
	fs.RemoveDirContents(dir)
}

func getTestFileDirPath() string {
	_, filename, _, _ := runtime.Caller(1)
	return path.Dir(filename)
}

func TestPathExists(t *testing.T) {
	p := path.Join(getTestFileDirPath(), "testResources/not-here")

	expected := false
	actual := pathExists(p)

	if actual != expected {
		t.Error("Expected pathExists to return", expected, "but it returned", actual)
	}

	p = path.Join(getTestFileDirPath(), "testResources/db")

	expected = true
	actual = pathExists(p)

	if actual != expected {
		t.Error("Expected pathExists to return", expected, "but it returned", actual)
	}

}

func TestNewFilePersistence_returns_an_error_when_given_an_invalid_dirpath(t *testing.T) {
	p := path.Join(getTestFileDirPath(), "testResources/not-here")

	_, err := newFilePersistence(p)

	if err == nil {
		t.Error("NewFilePersistence should return an error when given a path to a nonexitent directory")
	}
}

func TestNewFilePersistence_returns_a_struct_when_given_a_valid_path(t *testing.T) {
	dir := path.Join(getTestFileDirPath(), "testResources/db")
	fp, err := newFilePersistence(dir)

	if err != nil {
		t.Error("NewFilePersistence shouldn't return an error when given a valid path: ", dir)
	}
	if fp == nil {
		t.Error("NewFilePersistence should return a struct when given a valid dir path", dir)
	}
}

func TestFilePersistence_returns_an_error_when_key_doesnt_exist(t *testing.T) {
	fp := givenFilePersistence()
	_, err := fp.read("non-existent")
	if err == nil {
		t.Error("Expected to get an error, when trying to read a nonexistent entry")
	}
}

func TestFilePersistence_writes_and_reads_entries(t *testing.T) {
	fp := givenFilePersistence()

	err := fp.create("mykey1", "myvalue1")
	if err != nil {
		t.Error("Creating the first key value persistence caused an error:", err)
	}
	err = fp.create("mykey2", "myvalue2")
	if err != nil {
		t.Error("Creating the second key value persistence caused an error:", err)
	}

	expected := "myvalue1"
	actual, err := fp.read("mykey1")

	if err != nil {
		t.Error("Reading the first value caused an error:", err)
	}
	if actual != expected {
		t.Error("Expected to read value", expected, "but read", actual)
	}

	expected = "myvalue2"
	actual, err = fp.read("mykey2")

	if err != nil {
		t.Error("Reading the second value caused an error:", err)
	}
	if actual != expected {
		t.Error("Expected to read value", expected, "but read", actual)
	}
}

func TestFilePersistence_exists_returns_valid_values(t *testing.T) {
	fp := givenFilePersistence()

	expected := false
	actual := fp.(*filePersistence).exists("mykey0")

	if actual != expected {
		t.Error("Expected exists to return", expected, "but got", actual)
	}

	fp.create("mykey0", "myvalue0")
	expected = true
	actual = fp.(*filePersistence).exists("mykey0")

	if actual != expected {
		t.Error("Expected exists to return", expected, "but got", actual)
	}
}

func TestFilePersistence_updates_entries(t *testing.T) {
	fp := givenFilePersistence()

	fp.create("mykey1", "myvalue1")

	val1, _ := fp.read("mykey1")
	fp.update("mykey1", "new_value")
	val2, _ := fp.read("mykey1")

	if val1 == val2 {
		t.Error("Expected to get updated value, but didn't")
	}
}

func TestFilePersistence_deletes_entries(t *testing.T) {
	fp := givenFilePersistence()

	fp.create("mykey1", "myvalue1")

	if !fp.(*filePersistence).exists("mykey1") {
		t.Error("Expected mykey1 to exist, but it didn't")
	}

	fp.remove("mykey1")

	if fp.(*filePersistence).exists("mykey1") {
		t.Error("Expected mykey1 to be deleted, but it wasn't")
	}
}

func TestFilePersistence_createIfNonExistentElseUpdate_works(t *testing.T) {
	fp := givenFilePersistence()

	if fp.(*filePersistence).exists("cineeu-key") {
		t.Error("Expected cineeu-key not to exist, but it didn't")
	}

	fp.createIfNonExistentElseUpdate("cineeu-key", "value")

	if !fp.(*filePersistence).exists("cineeu-key") {
		t.Error("Expected cineeu-key to exist, but it didn't")
	}

	val1, _ := fp.read("cineeu-key")
	fp.createIfNonExistentElseUpdate("cineeu-key", "new-value")
	val2, _ := fp.read("cineeu-key")

	if val1 == val2 {
		t.Error("Expected to get updated value, but didn't")
	}
}

func givenFilePersistence() persistence {
	dir := path.Join(getTestFileDirPath(), "testResources/db")
	fp, _ := newFilePersistence(dir)
	return fp
}
