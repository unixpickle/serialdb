package serialdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"github.com/unixpickle/essentials"
	"github.com/unixpickle/serializer"
)

// WriteTable converts the objects into a Table and writes
// the results to w.
func WriteTable(w io.Writer, objs <-chan serializer.Serializer) (err error) {
	defer essentials.AddCtxTo("write table", &err)

	bufWriter := bufio.NewWriter(w)

	var offsets []int64
	var curOffset int64
	var serializerType string
	for obj := range objs {
		if serializerType == "" {
			serializerType = obj.SerializerType()
			typeLen := int32(len(serializerType))
			if err := binary.Write(bufWriter, byteOrder, typeLen); err != nil {
				return err
			}
			n, err := io.WriteString(bufWriter, serializerType)
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
		} else if err := binary.Write(bufWriter, byteOrder, int64(len(data))); err != nil {
			return err
		}
		if _, err := bufWriter.Write(data); err != nil {
			return err
		}
		offsets = append(offsets, curOffset)
		curOffset += int64(len(data) + 8)
	}
	if err := binary.Write(bufWriter, byteOrder, offsets); err != nil {
		return err
	}
	if err := binary.Write(bufWriter, byteOrder, int64(len(offsets))); err != nil {
		return err
	}
	return bufWriter.Flush()
}
