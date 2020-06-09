package operations

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/bsm/bfs/bfsfs"
	_ "github.com/bsm/bfs/bfsfs"
	_ "github.com/bsm/bfs/bfss3"
	"github.com/stretchr/testify/assert"
)

/*
 * RetrieveLatestSavepoint
 */
func TestRetrieveLatestSavepointShouldReturnAnErrorIfItCannotReadFromDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestRetrieveLatestSavepointShouldReturnAnErrorIfItCannotReadFromDir")
	defer os.RemoveAll(dir)
	assert.NoError(t, err)

	operator := RealOperator{}
	savepointPath := dir + "/savepoints"

	files, err := operator.retrieveLatestSavepoint(savepointPath)

	assert.Equal(t, "", files)
	assert.EqualError(t, err, "No savepoints present in directory: "+savepointPath)
}

func TestRetrieveLatestSavepointShouldReturnAnErrorIfFSSchemaNotSupported(t *testing.T) {
	operator := RealOperator{}

	files, err := operator.retrieveLatestSavepoint("tmpfs://")

	assert.Equal(t, "", files)
	assert.EqualError(t, err, "bfs: unknown URL scheme \"tmpfs\"")
}

func TestRetrieveLatestSavepointShouldReturnAnTheNewestFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestRetrieveLatestSavepointShouldReturnAnTheNewestFile")
	defer os.RemoveAll(dir)
	assert.NoError(t, err)

	fs, err := bfsfs.New(dir, "")
	f1, _ := fs.Create(context.Background(), "savepoint-683b3f-59401d30cfc4/_metadata", nil)
	defer f1.Discard()
	f1.Write([]byte("file a"))
	f1.Commit()
	time.Sleep(10 * time.Millisecond)
	f2, _ := fs.Create(context.Background(), "savepoint-323b3f-59401d30eoe6/_metadata", nil)
	defer f2.Discard()
	f2.Write([]byte("file b"))
	f2.Commit()

	operator := RealOperator{}

	files, err := operator.retrieveLatestSavepoint(dir)

	assert.Equal(t, dir+"/savepoint-323b3f-59401d30eoe6", files)
	assert.Nil(t, err)
}

func TestRetrieveLatestSavepointShouldRemoveTheTrailingSlashFromTheSavepointDirectory(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestRetrieveLatestSavepointShouldRemoveTheTrailingSlashFromTheSavepointDirectory")
	defer os.RemoveAll(dir)
	assert.NoError(t, err)

	fs, err := bfsfs.New(dir, "")
	f1, _ := fs.Create(context.Background(), "savepoint-683b3f-59401d30cfc4/_metadata", nil)
	defer f1.Discard()
	f1.Write([]byte("file a"))
	f1.Commit()
	time.Sleep(10 * time.Millisecond)
	f2, _ := fs.Create(context.Background(), "savepoint-323b3f-59401d30eoe6/_metadata", nil)
	defer f2.Discard()
	f2.Write([]byte("file b"))
	f2.Commit()
	operator := RealOperator{}

	files, err := operator.retrieveLatestSavepoint(dir + "/")

	assert.Equal(t, dir+"/savepoint-323b3f-59401d30eoe6", files)
	assert.Nil(t, err)
}
