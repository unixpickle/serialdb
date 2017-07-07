package serialdb

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/unixpickle/essentials"
	"github.com/unixpickle/serializer"
)

// WriteTable writes the objects to a Table file.
func WriteTable(w io.Writer, objs <-chan serializer.Serializer) (err error) {
	defer essentials.AddCtxTo("write tweet table", &err)
	var offsets []int64
	var curOffset int64
	var serializerType string
	for obj := range objs {
		if serializerType == "" {
			serializerType = obj.SerializerType()
			typeLen := int32(len(serializerType))
			if err := binary.Write(w, byteOrder, typeLen); err != nil {
				return err
			}
			n, err := io.WriteString(w, serializerType)
			if err != nil {
				return err
			}
			curOffset += int64(n) + 4
		} else if obj.SerializerType() != serializerType {
			return errors.New("mismatching serializer types: " +
				serializerType + " and " + obj.SerializerType())
		}

		data, err := obj.Serialize()
		if err != nil {
			return err
		} else if len(data) >= 1<<16 {
			return errors.New("tweet is too large")
		}
		if err := binary.Write(w, byteOrder, int16(len(data))); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		offsets = append(offsets, curOffset)
		curOffset += int64(len(data) + 2)
	}
	if err := binary.Write(w, byteOrder, offsets); err != nil {
		return err
	}
	return binary.Write(w, byteOrder, int64(len(offsets)))
}
