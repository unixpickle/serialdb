package serialdb

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/unixpickle/essentials"
	"github.com/unixpickle/serializer"
)

// WriteTableAny is like WriteTable, but the channel can
// be of any type as long as the objects are serializers.
func WriteTableAny(w io.Writer, objs interface{}) (err error) {
	converted := make(chan serializer.Serializer, 1)
	go func() {
		defer close(converted)
		val := reflect.ValueOf(objs)
		for {
			obj, ok := val.Recv()
			if !ok {
				return
			}
			converted <- obj.Interface().(serializer.Serializer)
		}
	}()
	return WriteTable(w, converted)
}

// GetAny reads a row from a table and writes the result
// into the value pointed to by out.
//
// The actual object type must be assignable to or
// convertible to the type of *out.
func GetAny(t Table, idx int64, out interface{}) (err error) {
	defer essentials.AddCtxTo("GetAny", &err)

	obj, err := t.Get(idx)
	if err != nil {
		return err
	}
	val := reflect.ValueOf(obj)
	destVal := reflect.ValueOf(out)

	if destVal.Kind() != reflect.Ptr {
		return errors.New("expected output to be a pointer")
	}
	if val.Type().AssignableTo(destVal.Type().Elem()) {
		destVal.Elem().Set(val)
	} else if val.Type().ConvertibleTo(destVal.Type().Elem()) {
		destVal.Elem().Set(val.Convert(destVal.Type().Elem()))
	} else {
		return fmt.Errorf("expecting %s but decoded %T",
			destVal.Type().Elem(), obj)
	}

	return nil
}
