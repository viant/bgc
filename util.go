package bgc

import (
	"fmt"
	"github.com/viant/dsc"
	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
	"time"
)

func waitForJobCompletion(service *bigquery.Service, context context.Context, projectID string, jobReferenceID string) (*bigquery.Job, error) {
	for range time.Tick(tickInterval) {
		statusCall := service.Jobs.Get(projectID, jobReferenceID)
		job, err := statusCall.Context(context).Do()
		if err != nil {
			return nil, fmt.Errorf("failed to check status %v", err)
		}
		if res := job.Status.ErrorResult; res != nil {
			return nil, fmt.Errorf("%v, reason: %v, locaction: %v", job.Status.ErrorResult.Message, job.Status.ErrorResult.Reason, job.Status.ErrorResult.Location)
		}
		if job.Status.State == doneStatus {
			return job, nil
		}
	}
	return nil, fmt.Errorf("failed to check job status")
}

func getServiceAndContext(connection dsc.Connection) (*bigquery.Service, context.Context, error) {
	client, err := asService(connection.Unwrap(servicePointer))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to unwrap biquery client:%v", err)
	}
	ctx, err := asContext(connection.Unwrap(contextPointer))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to unwrap ctx:%v", err)
	}
	return client, *ctx, nil
}

//GetServiceAndContextForManager returns big query service and context for passed in datastore manager.
func GetServiceAndContextForManager(manager dsc.Manager) (*bigquery.Service, context.Context, error) {
	provider := manager.ConnectionProvider()
	connection, err := provider.Get()
	if err != nil {
		return nil, nil, err
	}
	defer connection.Close()
	service, ctx, err := getServiceAndContext(connection)
	if err != nil {
		return nil, nil, err
	}
	return service, ctx, nil
}
