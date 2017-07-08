package serialdb

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/unixpickle/essentials"
	"github.com/unixpickle/serializer"
)

var byteOrder = binary.LittleEndian

// Table is a read-only table of Serializer objects with
// fast random accesses.
type Table interface {
	Len() int64
	Get(index int64) (serializer.Serializer, error)
	Close() error
}

// OpenTable opens a read-only table from f.
//
// If f is an io.Closer, then table.Close() will close f.
// On error, f will not be closed.
func OpenTable(f io.ReadSeeker) (table Table, err error) {
	defer essentials.AddCtxTo("open table", &err)

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var typeLength int32
	if err := binary.Read(f, byteOrder, &typeLength); err != nil {
		return nil, err
	}
	typeName := make([]byte, int(typeLength))
	if _, err := io.ReadFull(f, typeName); err != nil {
		return nil, err
	}
	deser := serializer.GetDeserializer(string(typeName))
	if deser == nil {
		return nil, errors.New("unknown type name: " + string(typeName))
	}

	endIdx, err := f.Seek(-8, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	var numEntries int64
	if err := binary.Read(f, byteOrder, &numEntries); err != nil {
		return nil, err
	}

	return &fileTable{
		file:          f,
		length:        numEntries,
		indicesOffset: endIdx - (numEntries * 8),
		deserializer:  deser,
	}, nil
}

// ReadTable reads all of the entries in a table
// asynchronously.
func ReadTable(ctx context.Context, f io.ReadSeeker) (<-chan serializer.Serializer,
	<-chan error) {
	outChan := make(chan serializer.Serializer, 1)
	errChan := make(chan error, 1)
	rawErrChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		for err := range rawErrChan {
			errChan <- essentials.AddCtx("read table", err)
		}
	}()

	go func() {
		defer close(rawErrChan)
		defer close(outChan)

		tableObj, err := OpenTable(f)
		if err != nil {
			rawErrChan <- err
			return
		}
		table := tableObj.(*fileTable)
		count := table.Len()

		off, err := table.objectOffset(0)
		if err != nil {
			rawErrChan <- err
			return
		}
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			rawErrChan <- err
			return
		}

		for i := int64(0); i < count; i++ {
			obj, err := table.readObject()
			if err != nil {
				rawErrChan <- err
				return
			}
			select {
			case outChan <- obj:
			case <-ctx.Done():
				rawErrChan <- ctx.Err()
				return
			}
		}
	}()

	return outChan, errChan
}

type fileTable struct {
	file          io.ReadSeeker
	length        int64
	indicesOffset int64
	deserializer  serializer.Deserializer
	seekLock      sync.Mutex
}

func (f *fileTable) Len() int64 {
	return f.length
}

func (f *fileTable) Get(index int64) (obj serializer.Serializer, err error) {
	defer essentials.AddCtxTo("read table entry", &err)

	f.seekLock.Lock()
	defer f.seekLock.Unlock()
	if index < 0 || index >= f.length {
		panic("index out of bounds")
	}

	objectOffset, err := f.objectOffset(index)
	if err != nil {
		return nil, err
	}
	if _, err := f.file.Seek(objectOffset, io.SeekStart); err != nil {
		return nil, err
	}
	return f.readObject()
}

func (f *fileTable) Close() error {
	if closer, ok := f.file.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (f *fileTable) objectOffset(index int64) (int64, error) {
	off := f.indicesOffset + 8*index
	if _, err := f.file.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	if err := binary.Read(f.file, byteOrder, &off); err != nil {
		return 0, err
	}
	return off, nil
}

func (f *fileTable) readObject() (serializer.Serializer, error) {
	var dataLen int64
	if err := binary.Read(f.file, byteOrder, &dataLen); err != nil {
		return nil, err
	}
	payload := make([]byte, int(dataLen))
	if _, err := io.ReadFull(f.file, payload); err != nil {
		return nil, err
	}
	return f.deserializer(payload)
}
