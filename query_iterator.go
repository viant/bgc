package bgc

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
)

var useLegacySQL = "/* USE LEGACY SQL */"
var queryPageSize = 500
var tickInterval = 100
var doneStatus = "DONE"

//QueryIterator represetns a QueryIterator.
type QueryIterator struct {
	*queryTask
	schema         *bigquery.TableSchema
	jobCompleted   bool
	jobReferenceID string
	Rows           []*bigquery.TableRow
	rowsIndex      uint64
	resultInfo     *QueryResultInfo
	pageToken      string
	totalRows      uint64
	processedRows  uint64
}

//HasNext returns true if there is next row to fetch.
func (qi *QueryIterator) HasNext() bool {
	return qi.processedRows < qi.totalRows
}

func convertRepeated(value []interface{}, field *bigquery.TableFieldSchema) (interface{}, error) {
	var result = []interface{}{}
	for _, item := range value {
		itemValue, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid repeated type, expected map[string]inerface{}, but had %T", item)
		}
		converted, err := convertValue(itemValue["v"], field)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)

	}
	return result, nil
}

func convertNested(value map[string]interface{}, field *bigquery.TableFieldSchema) (interface{}, error) {
	_, ok := value["f"]
	if !ok {
		return nil, fmt.Errorf("invalid nested for field: %v", field.Name)
	}
	nested, ok := value["f"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid nested nested type for field: %v", field.Name)
	}
	var fields = field.Fields
	if len(nested) != len(fields) {
		return nil, fmt.Errorf("schema length does not match nested length for field: %v", field.Name)
	}

	var result = map[string]interface{}{}
	for i, cell := range nested {
		cellValue, ok := cell.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid nested nested item type, expected map[string]interface{}, but had %T", cellValue)
		}
		converted, err := convertValue(cellValue["v"], fields[i])
		if err != nil {
			return nil, err
		}
		result[fields[i].Name] = converted
	}
	return result, nil
}

func convertValue(value interface{}, field *bigquery.TableFieldSchema) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	switch typedValue := value.(type) {
	case []interface{}:
		return convertRepeated(typedValue, field)
	case map[string]interface{}:
		return convertNested(typedValue, field)
	}

	switch strings.ToUpper(field.Type) {
	case "INTEGER":
		return toolbox.ToInt(value)
	case "FLOAT":
		return toolbox.ToFloat(value)
	case "TIMESTAMP":
		timestampFloat, err := toolbox.ToFloat(value)
		if err != nil {
			return nil, err
		}
		timestamp := int64(timestampFloat*1000) * int64(time.Millisecond)
		timeValue := time.Unix(0, timestamp)
		return timeValue, nil
	case "BOOLEAN":
		return toolbox.AsBoolean(value), nil
	}
	return value, nil
}

//Next returns next row.
func (qi *QueryIterator) Next() ([]interface{}, error) {
	if int(qi.rowsIndex) >= len(qi.Rows) {
		err := qi.fetchPage()
		if err != nil {
			return nil, err
		}
	}
	row := qi.Rows[qi.rowsIndex]
	qi.processedRows++
	qi.rowsIndex++
	fields := qi.schema.Fields
	var values = make([]interface{}, 0)
	for i, cell := range row.F {
		value, err := convertValue(cell.V, fields[i])
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (qi *QueryIterator) fetchPage() error {
	queryResultCall := qi.service.Jobs.GetQueryResults(qi.projectID, qi.jobReferenceID)

	pageSize := qi.manager.Config().GetInt("pageSize", queryPageSize)

	queryResultCall.MaxResults(int64(pageSize)).PageToken(qi.pageToken)

	jobGetResult, err := queryResultCall.Context(qi.context).Do()
	if err != nil {
		return err
	}
	if qi.totalRows == 0 {
		qi.totalRows = jobGetResult.TotalRows
	}
	if qi.resultInfo.TotalRows == 0 {
		qi.resultInfo.TotalBytesProcessed = int(jobGetResult.TotalBytesProcessed)
		qi.resultInfo.CacheHit = jobGetResult.CacheHit
		qi.resultInfo.TotalRows = int(jobGetResult.TotalRows)
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

	config := manager.Config()
	var result = &QueryIterator{

		Rows: make([]*bigquery.TableRow, 0),
		queryTask: &queryTask{
			manager:   manager,
			service:   service,
			context:   context,
			projectID: config.Get(ProjectIDKey),
			datasetID: config.Get(DataSetIDKey),
		},
	}
	job, err := result.run(query)
	if err != nil {
		fmt.Printf("err:%v\n%v", err, query)
	}
	if err != nil {
		return nil, err
	}
	result.jobReferenceID = job.JobReference.JobId
	result.resultInfo = &QueryResultInfo{}
	err = result.fetchPage()

	if err != nil {
		return nil, err
	}
	return result, nil
}
