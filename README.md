# serialdb [![GoDoc](https://godoc.org/github.com/unixpickle/serialdb?status.svg)](https://godoc.org/github.com/unixpickle/serialdb)

Package serialdb implements a fast read-only database for [serializer](https://github.com/unixpickle/serializer) objects. The main benefits:

 * Fast random access
 * Don't load a lot of stuff into RAM
