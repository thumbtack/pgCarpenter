package storage

import (
	"io"
)

type Storage interface {
	// Put stores the contents of the local file path in the object identified by key. It also
	// stores the last modified timestamp (mtime) in the object's metadata.
	Put(key string, localPath string, mtime int64) error
	// PutString stores the value of body as the content of the object identified by key.
	PutString(key string, body string) error
	// Get writes the contents of the object identified by key into out.
	Get(key string, out io.WriterAt) error
	// GetString returns the contents of the object as a string.
	GetString(key string) (string, error)
	// GetLastModifiedTime returns the modified time as stored in the objects metadata.
	GetLastModifiedTime(key string) (int64, error)
	// ListFolder returns the contents (list of strings) of the folder rooted at path.
	ListFolder(path string) ([]string, error)
	// WalkFolder traverses the folder rooted at path, putting each object it finds in the channel keysC.
	// If an error occurs the traversal is interrupted and the error returned.
	WalkFolder(path string, keysC chan<- string) error
}
