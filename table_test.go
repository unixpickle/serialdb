package serialdb

import (
	"bytes"
	"context"
	"math/rand"
	"reflect"
	"testing"

	"github.com/unixpickle/serializer"
)

func init() {
	serializer.RegisterTypedDeserializer((&serialObject{}).SerializerType(),
		DeserializeSerialObject)
}

func TestTable(t *testing.T) {
	data, objects := testingTableData(t)
	reader := bytes.NewReader(data)
	table, err := OpenTable(reader)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	if table.Len() != int64(len(objects)) {
		t.Fatalf("expected length %v but got %v", len(objects), table.Len())
	}

	for _, j := range rand.Perm(len(objects)) {
		entry, err := table.Get(int64(j))
		if err != nil {
			t.Error(err)
		} else if !reflect.DeepEqual(entry, objects[j]) {
			t.Errorf("index %d: expected %#v but got %#v", j, objects[j], entry)
		}
		var someObj *serialObject
		if err := GetAny(table, int64(j), &someObj); err != nil {
			t.Error(err)
		}
		if err != nil {
			t.Error(err)
		} else if !reflect.DeepEqual(someObj, objects[j]) {
			t.Errorf("index %d: expected %#v but got %#v", j, objects[j], someObj)
		}
	}
}

func TestReadTable(t *testing.T) {
	data, objects := testingTableData(t)
	reader := bytes.NewReader(data)
	stream, errChan := ReadTable(context.Background(), reader)
	var readObjects []*serialObject
	for obj := range stream {
		readObjects = append(readObjects, obj.(*serialObject))
	}
	if err := <-errChan; err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(objects, readObjects) {
		t.Errorf("expected %#v but got %#v", objects, readObjects)
	}
}

func testingTableData(t *testing.T) ([]byte, []*serialObject) {
	var buf bytes.Buffer
	objects := []*serialObject{
		{Name: "joe", ID: "123", Body: "i love monkeys"},
		{Name: "james", ID: "321", Body: "joe loves monkeys"},
		{Name: "alex", ID: "1337", Body: "james knows joe loves monkeys"},
		{Name: "jon", ID: "666", Body: "alex hates monkeys"},
		{Name: "dave", ID: "111", Body: "I am a monkey"},
		{Name: "james", ID: "222", Body: "I am a bot"},
		{Name: "bill", ID: "123123123", Body: "my spirit animal is a lion"},
		{Name: "bob", ID: "surprise!", Body: "i despise tacos"},
		{Name: "steve", ID: "", Body: "and i despise bob"},
	}
	objChan := make(chan *serialObject, len(objects))
	for _, o := range objects {
		objChan <- o
	}
	close(objChan)
	if err := WriteTableAny(&buf, objChan); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), objects
}

type serialObject struct {
	Name string
	ID   string
	Body string
}

func DeserializeSerialObject(d []byte) (*serialObject, error) {
	var s serialObject
	if err := serializer.DeserializeAny(d, &s.Name, &s.ID, &s.Body); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *serialObject) SerializerType() string {
	return "github.com/unixpickle/serialdb"
}

func (s *serialObject) Serialize() ([]byte, error) {
	return serializer.SerializeAny(s.Name, s.ID, s.Body)
}
