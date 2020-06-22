package operations

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/bsm/bfs/bfsfs"
	"github.com/stretchr/testify/assert"
	"github.com/therako/flink-deployer/cmd/cli/flink"
)

/*
 * filterRunningJobsByName
 */
func TestFilterRunningJobsShouldReturnAnEmptySliceIfNoJobsWereProvided(t *testing.T) {
	operator := RealOperator{}

	res := operator.filterRunningJobsByName([]flink.Job{}, "test")

	assert.Len(t, res, 0)
}

func TestFilterRunningJobsShouldReturnAnEmptySliceIfNoJobsAreRunning(t *testing.T) {
	operator := RealOperator{}

	res := operator.filterRunningJobsByName(
		[]flink.Job{
			{
				Status: "STOPPED",
			},
		},
		"test",
	)
	assert.Len(t, res, 0)
}

func TestFilterRunningJobsShouldReturnTheRunningJobs(t *testing.T) {
	operator := RealOperator{}

	res := operator.filterRunningJobsByName(
		[]flink.Job{
			{
				Status: "STOPPED",
				Name:   "test",
			},
			{
				Status: "RUNNING",
				Name:   "test",
			},
		},
		"test",
	)

	assert.Len(t, res, 1)
}

func TestFilterRunningJobsShouldReturnTheRunningJobsMatchingTheJobBaseName(t *testing.T) {
	operator := RealOperator{}

	res := operator.filterRunningJobsByName(
		[]flink.Job{
			{
				Status: "STOPPED",
				Name:   "jobA v1.0",
			},
			{
				Status: "RUNNING",
				Name:   "JobB",
			},
			{
				Status: "RUNNING",
				Name:   "jobA v1.1",
			},
		},
		"jobA",
	)

	assert.Len(t, res, 1)
}

/*
 * monitorSavepointCreation
 */
func TestMonitorSavepointCreationShouldReturnAnErrorWhenTheSavepointFailsToBeCreated(t *testing.T) {
	mockedMonitorSavepointCreationError = errors.New("failed")

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.monitorSavepointCreation("job-id", "request-id", 1)

	assert.EqualError(t, err, "failed to create savepoint for job \"job-id\" within 1 seconds")
}

func TestMonitorSavepointCreationShouldReturnNilWhenTheSavepointIsCreated(t *testing.T) {
	mockedMonitorSavepointCreationError = nil
	mockedMonitorSavepointCreationResponse = flink.MonitorSavepointCreationResponse{
		Status: flink.SavepointCreationStatus{
			Id: "COMPLETED",
		},
	}

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.monitorSavepointCreation("job-id", "request-id", 1)

	assert.NoError(t, err)
}

/*
 * UpdateJob
 */
func TestUpdateJobShouldReturnAnErrorWhenTheJobNameBaseIsUndefined(t *testing.T) {
	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		LocalFilename: "testdata/sample.jar",
	})

	assert.EqualError(t, err, "unspecified argument 'JobNameBase'")
}

func TestUpdateJobShouldReturnAnErrorWhenTheSavepointDirectoryIsUndefined(t *testing.T) {
	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		JobNameBase:   "WordCountStateful",
		LocalFilename: "testdata/sample.jar",
	})

	assert.EqualError(t, err, "unspecified argument 'SavepointDir'")
}

func TestUpdateJobShouldReturnAnErrorWhenRetrievingTheJobsFails(t *testing.T) {
	mockedRetrieveJobsError = errors.New("failed")

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		JobNameBase:   "WordCountStateful",
		LocalFilename: "testdata/sample.jar",
		SavepointDir:  "/data/flink",
	})

	assert.EqualError(t, err, "retrieving jobs failed: failed")
}

func TestUpdateJobShouldReturnAnErrorWhenTheSavepointCannotBeCreated(t *testing.T) {
	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{
		{
			ID:     "Job-A",
			Name:   "WordCountStateful v1.0",
			Status: "RUNNING",
		},
	}
	mockedCreateSavepointError = errors.New("failed")

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		// Use the same job name as the mock job above
		// operator.Update will filter running jobs by name to cancel.
		JobNameBase:   "WordCountStateful v1.0",
		LocalFilename: "testdata/sample.jar",
		SavepointDir:  "/data/flink",
	})

	assert.EqualError(t, err, "failed to create savepoint for job Job-A due to error: failed")
}

func TestUpdateJobShouldReturnAnErrorWhenTheJobCannotBeCanceled(t *testing.T) {
	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{
		{
			ID:     "Job-A",
			Name:   "WordCountStateful v1.0",
			Status: "RUNNING",
		},
	}
	mockedCreateSavepointError = nil
	mockedCreateSavepointResponse = flink.CreateSavepointResponse{
		RequestID: "request-id",
	}
	mockedMonitorSavepointCreationResponse = flink.MonitorSavepointCreationResponse{
		Status: flink.SavepointCreationStatus{
			Id: "COMPLETED",
		},
	}
	mockedTerminateError = errors.New("failed")

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		JobNameBase:   "WordCountStateful v1.0",
		LocalFilename: "../testdata/sample.jar",
		SavepointDir:  "/data/flink",
	})

	assert.EqualError(t, err, "job \"Job-A\" failed to cancel due to: failed")
}

func TestUpdateJobShouldReturnAnErrorWhenTheLatestSavepointCannotBeRetrieved(t *testing.T) {
	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{
		{
			ID:     "Job-A",
			Name:   "WordCountStateful v1.0",
			Status: "RUNNING",
		},
	}
	mockedCreateSavepointError = nil
	mockedCreateSavepointResponse = flink.CreateSavepointResponse{
		RequestID: "request-id",
	}
	mockedMonitorSavepointCreationResponse = flink.MonitorSavepointCreationResponse{
		Status: flink.SavepointCreationStatus{
			Id: "COMPLETED",
		},
	}
	mockedTerminateError = nil
	mockedUploadJarResponse = flink.UploadJarResponse{
		Filename: "/data/flink/sample.jar",
		Status:   "success",
	}
	mockedRunJarError = nil

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		JobNameBase:   "WordCountStateful v1.0",
		LocalFilename: "../testdata/sample.jar",
		SavepointDir:  "/data/flink",
	})

	assert.EqualError(t, err, "retrieving the latest savepoint failed: No savepoints present in directory: /data/flink")
}

func TestUpdateJobShouldReturnNilWhenTheUpdateSucceeds(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	defer os.RemoveAll(dir)
	assert.NoError(t, err)

	fs, err := bfsfs.New(dir, "")
	f1, _ := fs.Create(context.Background(), "savepoint-683b3f-59401d30cfc4/_metadata", nil)
	defer f1.Discard()
	f1.Write([]byte("file a"))
	f1.Commit()

	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{
		{
			ID:     "Job-A",
			Name:   "WordCountStateful v1.0",
			Status: "RUNNING",
		},
	}
	mockedCreateSavepointError = nil
	mockedCreateSavepointResponse = flink.CreateSavepointResponse{
		RequestID: "request-id",
	}
	mockedMonitorSavepointCreationResponse = flink.MonitorSavepointCreationResponse{
		Status: flink.SavepointCreationStatus{
			Id: "COMPLETED",
		},
	}
	mockedTerminateError = nil
	mockedUploadJarResponse = flink.UploadJarResponse{
		Filename: "/data/flink/sample.jar",
		Status:   "success",
	}
	mockedRunJarError = nil

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err = operator.Update(UpdateJob{
		JobNameBase:   "WordCountStateful v1.0",
		LocalFilename: "../testdata/sample.jar",
		SavepointDir:  dir,
	})

	assert.NoError(t, err)
}

func TestUpdateJobShouldReturnAnErrorWhenNoRunningJobsAreFound(t *testing.T) {
	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{}

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		JobNameBase:   "WordCountStateful",
		LocalFilename: "testdata/sample.jar",
		SavepointDir:  "/data/flink",
	})

	assert.EqualError(t, err, "no instance running for job name base \"WordCountStateful\". Aborting update")
}

func TestUpdateJobShouldReturnErrorWhenNoRunningJobsAreFoundAndFallbackToDeployIsFalse(t *testing.T) {
	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{}

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err := operator.Update(UpdateJob{
		JobNameBase:      "WordCountStateful",
		LocalFilename:    "testdata/sample.jar",
		SavepointDir:     "/data/flink",
		FallbackToDeploy: false,
	})

	assert.EqualError(t, err, "no instance running for job name base \"WordCountStateful\". Aborting update")
}

func TestUpdateJobShouldFallbackToDeployFromSavepointWhenNoRunningJobsAreFound(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	defer os.RemoveAll(dir)
	assert.NoError(t, err)

	fs, err := bfsfs.New(dir, "")
	f1, _ := fs.Create(context.Background(), "savepoint-683b3f-59401d30cfc4/_metadata", nil)
	defer f1.Discard()
	f1.Write([]byte("file a"))
	f1.Commit()

	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{}

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	err = operator.Update(UpdateJob{
		JobNameBase:      "WordCountStateful",
		LocalFilename:    "testdata/sample.jar",
		SavepointDir:     dir,
		FallbackToDeploy: true,
	})

	assert.NoError(t, err)
}

func TestUpdateJobShouldReturnAnErrorWhenMultipleRunningJobsAreFound(t *testing.T) {
	mockedRetrieveJobsError = nil
	mockedRetrieveJobsResponse = []flink.Job{
		{
			ID:     "Job-A",
			Name:   "WordCountStateful v1.0",
			Status: "RUNNING",
		},
		{
			ID:     "Job-B",
			Name:   "WordCountStateful v1.0",
			Status: "RUNNING",
		},
	}

	operator := RealOperator{
		FlinkRestAPI: TestFlinkRestClient{
			BaseURL: "http://localhost",
			Client:  &http.Client{},
		},
	}

	// flink-deployer wont want to update (stop/start) an undesired job
	// when there are two running jobs with same name. So it must abort the update
	err := operator.Update(UpdateJob{
		JobNameBase:   "WordCountStateful",
		LocalFilename: "testdata/sample.jar",
		SavepointDir:  "/data/flink",
	})

	assert.EqualError(t, err, "job name with base \"WordCountStateful\" has 2 instances running. Aborting update")
}
