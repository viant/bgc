package bgc

import (
	"errors"
	"strings"
	"time"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
)

var useLegacySQL = "/* USE LEGACY SQL */"
var queryPageSize int64 = 500
var tickInterval = 100 * time.Millisecond
var doneStatus = "DONE"

//QueryIterator represetns a QueryIterator.
type QueryIterator struct {
	service        *bigquery.Service
	context        context.Context
	schema         *bigquery.TableSchema
	projectID      string
	jobCompleted   bool
	jobReferenceID string
	Rows           []*bigquery.TableRow
	rowsIndex      uint64
	pageToken      string
	totalRows      uint64
	processedRows  uint64
}

//HasNext returns true if there is next row to fetch.
func (qi *QueryIterator) HasNext() bool {
	return qi.processedRows < qi.totalRows
}

//Unwarpping big query nested result
func unwrapValueIfNeeded(value interface{}, field *bigquery.TableFieldSchema) interface{} {
	if fieldMap, ok := value.(map[string]interface{}); ok {
		if wrappedValue, ok := fieldMap["v"]; ok {
			if field.Fields != nil {
				return unwrapValueIfNeeded(wrappedValue, field)
			}
			return wrappedValue
		}
		if fieldValue, ok := fieldMap["f"]; ok {
			unwrapped := unwrapValueIfNeeded(fieldValue, field)
			if field.Fields != nil {
				index := 0
				var newMap = make(map[string]interface{})
				toolbox.ProcessSlice(unwrapped, func(item interface{}) bool {
					newMapValue := unwrapValueIfNeeded(item, field.Fields[index])
					newMap[field.Fields[index].Name] = newMapValue
					index++
					return true
				})
				return newMap
			}
		}
	}
	if slice, ok := value.([]interface{}); ok {
		var newSlice = make([]interface{}, 0)
		for _, item := range slice {
			value := unwrapValueIfNeeded(item, field)
			newSlice = append(newSlice, value)
		}
		return newSlice
	}
	return value
}

func toValue(source interface{}, field *bigquery.TableFieldSchema) interface{} {
	switch sourceValue := source.(type) {
	case []interface{}:
		var newSlice = make([]interface{}, 0)
		for _, item := range sourceValue {
			itemValue := unwrapValueIfNeeded(item, field)
			newSlice = append(newSlice, itemValue)
		}
		return newSlice

	case map[string]interface{}:
		return unwrapValueIfNeeded(sourceValue, field)

	}

	return source

}

//Next returns next row.
func (qi *QueryIterator) Next() ([]interface{}, error) {
	qi.processedRows++
	qi.rowsIndex++
	if int(qi.rowsIndex) >= len(qi.Rows) {
		err := qi.fetchPage()
		if err != nil {
			return nil, err
		}
	}
	row := qi.Rows[qi.rowsIndex]
	fields := qi.schema.Fields
	var values = make([]interface{}, 0)
	for i, cell := range row.F {
		value := toValue(cell.V, fields[i])
		values = append(values, value)
	}
	return values, nil
}

func (qi *QueryIterator) fetchPage() error {
	queryResultCall := qi.service.Jobs.GetQueryResults(qi.projectID, qi.jobReferenceID)
	queryResultCall.MaxResults(queryPageSize).PageToken(qi.pageToken)
	jobGetResult, err := queryResultCall.Context(qi.context).Do()
	if err != nil {
		return err
	}
	if qi.totalRows == 0 {
		qi.totalRows = jobGetResult.TotalRows
	}
	qi.rowsIndex = 0
	qi.Rows = jobGetResult.Rows
	qi.pageToken = jobGetResult.PageToken
	qi.jobCompleted = jobGetResult.JobComplete
	if qi.schema == nil {
		qi.schema = jobGetResult.Schema
	}
	return nil
}

//GetColumns returns query columns, after query executed.
func (qi *QueryIterator) GetColumns() ([]string, error) {
	if qi.schema == nil {
		return nil, errors.New("Failed to get table schema")
	}
	var result = make([]string, 0)
	for _, field := range qi.schema.Fields {
		result = append(result, field.Name)
	}
	return result, nil
}

//NewQueryIterator creates a new query iterator for passed in datastore manager and query.
func NewQueryIterator(manager dsc.Manager, query string) (*QueryIterator, error) {
	service, context, err := GetServiceAndContextForManager(manager)
	if err != nil {
		return nil, err
	}

	useLegacySQL := strings.Contains(query, useLegacySQL) && !strings.Contains(strings.ToLower(query), "group by")

	config := manager.Config()

	projectID := config.Get("projectId")
	datasetID := config.Get("datasetId")

	datasetReference := &bigquery.DatasetReference{ProjectId: projectID, DatasetId: datasetID}
	jobConfigurationQuery := &bigquery.JobConfigurationQuery{
		Query:          query,
		DefaultDataset: datasetReference,
	}

	if !useLegacySQL {
		jobConfigurationQuery.UseLegacySql = false
		jobConfigurationQuery.ForceSendFields = []string{"UseLegacySql"}
	}

	jobConfiguration := &bigquery.JobConfiguration{Query: jobConfigurationQuery}
	queryJob := bigquery.Job{Configuration: jobConfiguration}
	jobCall := service.Jobs.Insert(projectID, &queryJob)

	postedJob, err := jobCall.Context(context).Do()
	if err != nil {
		return nil, err
	}
	responseJob, err := waitForJobCompletion(service, context, projectID, postedJob.JobReference.JobId)
	if err != nil {
		return nil, err
	}
	result := &QueryIterator{
		service:        service,
		context:        context,
		projectID:      projectID,
		jobReferenceID: responseJob.JobReference.JobId,
		Rows:           make([]*bigquery.TableRow, 0),
	}
	err = result.fetchPage()
	if err != nil {
		return nil, err
	}
	return result, nil
}
