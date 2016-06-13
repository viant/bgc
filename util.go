/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */
package bgc

import (
	"fmt"
	"time"

	"github.com/viant/dsc"
	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
)

func waitForJobCompletion(service *bigquery.Service, context context.Context, projectID string, jobReferenceID string) (*bigquery.Job, error) {
	for range time.Tick(tickInterval) {
		statusCall := service.Jobs.Get(projectID, jobReferenceID)
		job, err := statusCall.Context(context).Do()
		if err != nil {
			return nil, fmt.Errorf("Failed to check job status due to %v", err)
		}
		if job.Status.State == doneStatus {
			return job, nil
		}
	}
	return nil, fmt.Errorf("Failed to check job status")
}

func getServiceAndContext(connection dsc.Connection) (*bigquery.Service, context.Context, error) {
	client, err := asService(connection.Unwrap(servicePointer))
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to unwrap biquery client:%v", err)
	}
	context, err := asContext(connection.Unwrap(contextPointer))
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to unwrap context:%v", err)
	}
	return client, *context, nil
}

//GetServiceAndContextForManager returns big query service and context for passed in datastore manager.
func GetServiceAndContextForManager(manager dsc.Manager) (*bigquery.Service, context.Context, error) {
	provider := manager.ConnectionProvider()
	connection, err := provider.Get()
	if err != nil {
		return nil, nil, err
	}
	defer connection.Close()
	service, context, err := getServiceAndContext(connection)
	if err != nil {
		return nil, nil, err
	}
	return service, context, nil
}
