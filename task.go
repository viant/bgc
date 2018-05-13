package bgc

import (
	"context"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

type queryTask struct {
	projectID string
	datasetID string
	service   *bigquery.Service
	context   context.Context
}

func (t *queryTask) run(query string) (*bigquery.Job, error) {
	jobConfigurationQuery := &bigquery.JobConfigurationQuery{
		Query:          query,
		DefaultDataset: &bigquery.DatasetReference{ProjectId: t.projectID, DatasetId: t.datasetID},
	}
	falseValue := false
	useLegacy := strings.Contains(query, useLegacySQL)
	jobConfigurationQuery.UseLegacySql = &falseValue
	if useLegacy {
		trueValue := true
		jobConfigurationQuery.UseLegacySql = &trueValue
		jobConfigurationQuery.ForceSendFields = []string{"UseLegacySql"}
	}
	jobConfiguration := &bigquery.JobConfiguration{Query: jobConfigurationQuery}
	queryJob := bigquery.Job{Configuration: jobConfiguration}
	jobCall := t.service.Jobs.Insert(t.projectID, &queryJob)
	postedJob, err := jobCall.Context(t.context).Do()
	if err != nil {
		return nil, err
	}

	postedJob, err = waitForJobCompletion(t.service, t.context, t.projectID, postedJob.JobReference.JobId)
	if err != nil {
		return nil, err
	}
	return postedJob, err
}
