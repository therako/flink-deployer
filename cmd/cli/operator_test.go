package main

import (
	"github.com/therako/flink-deployer/cmd/cli/flink"
	"github.com/therako/flink-deployer/cmd/cli/operations"
)

var mockedDeployError error
var mockedUpdateError error
var mockedTerminateError error
var mockedRetrieveJobsResponse []flink.Job
var mockedRetrieveJobsError error

type TestOperator struct {
	FlinkRestAPI flink.FlinkRestAPI
}

func (t TestOperator) Deploy(d operations.Deploy) error {
	return mockedDeployError
}

func (t TestOperator) Update(u operations.UpdateJob) error {
	return mockedUpdateError
}

func (t TestOperator) Terminate(te operations.TerminateJob) error {
	return mockedUpdateError
}

func (t TestOperator) RetrieveJobs() ([]flink.Job, error) {
	return mockedRetrieveJobsResponse, mockedRetrieveJobsError
}
