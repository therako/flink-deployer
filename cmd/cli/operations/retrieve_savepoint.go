package operations

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/bsm/bfs"
)

const (
	defaultFSPrefix   = "file://"
	flinkMetafilePath = "/_metadata"
)

func (o RealOperator) retrieveLatestSavepoint(dir string) (string, error) {
	if !strings.Contains(dir, "://") {
		dir = defaultFSPrefix + dir
	}
	fs, err := bfs.Connect(context.Background(), dir)
	if err != nil {
		return "", err
	}

	if strings.HasSuffix(dir, "/") {
		dir = strings.TrimSuffix(dir, "/")
	}

	filesIterator, err := fs.Glob(context.Background(), "*"+flinkMetafilePath)
	if err != nil {
		return "", err
	}
	defer filesIterator.Close()

	var newestFile string
	var newestTime time.Time
	for filesIterator.Next() {
		filePath := filesIterator.Name()
		currTime := filesIterator.ModTime()
		if currTime.After(newestTime) {
			newestTime = currTime
			newestFile = filePath
		}
	}

	if newestFile == "" {
		return "", errors.New("No savepoints present in directory: " + strings.TrimPrefix(dir, defaultFSPrefix))
	}

	newestFile = strings.TrimPrefix(dir, defaultFSPrefix) + "/" + strings.TrimSuffix(newestFile, flinkMetafilePath)
	return newestFile, nil
}
