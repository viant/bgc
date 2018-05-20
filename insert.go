package bgc

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"io"
	"log"
	"time"
)

const (
	InsertMethodStream = "stream"
	InsertMethodLoad   = "load"
	jsonFormat         = "NEWLINE_DELIMITED_JSON"
	createIfNeeded     = "CREATE_IF_NEEDED"
	writeAppend        = "WRITE_APPEND"
)

var pullDuration = 5 * time.Second
var streamingTimeout = 120 * time.Second

//InsertTask represents insert streaming task.
type InsertTask struct {
	tableDescriptor   *dsc.TableDescriptor
	service           *bigquery.Service
	context           context.Context
	projectID         string
	datasetID         string
	waitForCompletion bool
	manager           dsc.Manager
	insertMethod      string
}

//InsertSingle streams single records into big query.
func (it *InsertTask) InsertSingle(record map[string]interface{}) error {
	_, err := it.InsertAll([]map[string]interface{}{record})
	return err
}

func (it *InsertTask) insertID(record map[string]interface{}) string {
	pkValue := ""
	for _, pkColumn := range it.tableDescriptor.PkColumns {
		pkValue = pkValue + toolbox.AsString(record[pkColumn])
	}
	return pkValue
}

//normalizeValue rewrites data structure and remove nil values,
func normalizeValue(value interface{}) (interface{}, bool) {
	if value == nil {
		return nil, false
	}
	if val, ok := value.(interface{}); !ok || val == nil {
		return nil, false
	}
	value = toolbox.DereferenceValue(value)
	switch val := value.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, bool, float64, float32:
		return val, true
	}
	if toolbox.IsStruct(value) {
		return normalizeValue(toolbox.AsMap(value))
	} else if toolbox.IsMap(value) {
		aMap := toolbox.AsMap(value)
		for k, v := range aMap {
			val, ok := normalizeValue(v)
			if !ok {
				delete(aMap, k)
				continue
			}
			aMap[k] = val
		}
		return aMap, len(aMap) > 0

	} else if toolbox.IsSlice(value) {
		aSlice := toolbox.AsSlice(value)
		newSlice := []interface{}{}
		for _, item := range aSlice {
			if val, ok := normalizeValue(item); ok {
				newSlice = append(newSlice, val)
			}
		}
		return newSlice, len(newSlice) > 0
	}
	return value, true
}

func asJsonMap(record map[string]interface{}) map[string]bigquery.JsonValue {
	var jsonValues = make(map[string]bigquery.JsonValue)
	for k, v := range record {
		val, ok := normalizeValue(v)
		if !ok {
			continue
		}
		jsonValues[k] = val

	}
	return jsonValues
}

func (it *InsertTask) getTableRowCount() (int, error) {
	iterator, err := NewQueryIterator(it.manager, "SELECT COUNT(*) AS cnt FROM "+it.tableDescriptor.Table)
	if err != nil {
		return 0, err
	}
	if iterator.HasNext() {
		record, err := iterator.Next()
		if err != nil {
			return 0, err
		}
		return toolbox.AsInt(toolbox.AsString(record[0])), nil
	}
	return 0, nil
}

func (it *InsertTask) waitForInsertCompletion(streamRowCount, initialRowCount int) error {
	var maxCount = int(streamingTimeout / pullDuration)
	for i := 0; i < maxCount; i++ {
		recentTableRowCount, err := it.getTableRowCount()
		if err != nil {
			return err
		}
		if recentTableRowCount != initialRowCount {
			return nil
		}
		time.Sleep(pullDuration)
		if i > 0 {
			log.Printf("Waiting for stream data %v(%v) being available: elapsed %v sec ", it.tableDescriptor.Table, streamRowCount, ((i + 1) * int(pullDuration/time.Second)))
		}
	}

	return fmt.Errorf("timeout - unable to check streaming data in big query")
}

func buildRecord(record map[string]interface{}) map[string]interface{} {
	result := data.NewMap()
	for k, v := range toolbox.AsMap(record) {
		result.SetValue(k, v)
	}
	return toolbox.DeleteEmptyKeys(result)
}

func (it *InsertTask) buildLoadData(records []map[string]interface{}) (io.Reader, error) {
	result := new(bytes.Buffer)
	writer := gzip.NewWriter(result)
	for _, item := range records {
		err := json.NewEncoder(writer).Encode(asJsonMap(item))
		if err != nil {
			return nil, err
		}
	}
	var err error
	if err = writer.Flush(); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return bytes.NewReader(result.Bytes()), nil
}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) LoadAll(records []map[string]interface{}) (int, error) {
	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: &bigquery.JobConfigurationLoad{
				SourceFormat: jsonFormat,
				DestinationTable: &bigquery.TableReference{
					ProjectId: it.projectID,
					DatasetId: it.datasetID,
					TableId:   it.tableDescriptor.Table,
				},
				CreateDisposition: createIfNeeded,
				WriteDisposition:  writeAppend,
			},
		},
	}
	call := it.service.Jobs.Insert(it.projectID, job)
	mediaReader, err := it.buildLoadData(records)
	if err != nil {
		return 0, err
	}

	call = call.Media(mediaReader, googleapi.ContentType("application/octet-stream"))

	job, err = call.Do()
	if err != nil {
		return 0, err
	}
	_, err = waitForJobCompletion(it.service, it.context, it.projectID, job.JobReference.JobId)
	return len(records), err
}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) StreamAll(records []map[string]interface{}) (int, error) {
	insertRequest := &bigquery.TableDataInsertAllRequest{}
	var insertRequestRows = make([]*bigquery.TableDataInsertAllRequestRows, len(records))
	for i, record := range records {
		record := buildRecord(toolbox.AsMap(record))
		insertRequestRows[i] = &bigquery.TableDataInsertAllRequestRows{InsertId: it.insertID(record), Json: asJsonMap(record)}
	}
	insertRequest.Rows = insertRequestRows
	streamRowCount := len(insertRequestRows)
	var initialRowCount = 0

	if it.waitForCompletion {
		var err error
		initialRowCount, err = it.getTableRowCount()
		if err != nil {
			return 0, err
		}
	}

	requestCall := it.service.Tabledata.InsertAll(it.projectID, it.datasetID, it.tableDescriptor.Table, insertRequest)
	response, err := requestCall.Context(it.context).Do()
	if err != nil {
		return 0, err
	}

	if response.InsertErrors != nil && len(response.InsertErrors) > 0 {
		return 0, fmt.Errorf("failed to insert records %v", response.InsertErrors[0].Errors[0])
	}

	if it.waitForCompletion {
		err := it.waitForInsertCompletion(streamRowCount, initialRowCount)
		if err != nil {
			return streamRowCount, err
		}
	}
	return streamRowCount, nil
}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) InsertAll(records []map[string]interface{}) (int, error) {
	if it.insertMethod == InsertMethodStream {
		return it.StreamAll(records)
	}
	return it.LoadAll(records)
}

//NewInsertTask creates a new streaming insert task, it takes manager, table descript with schema, waitForCompletion flag with time duration.
func NewInsertTask(manager dsc.Manager, table *dsc.TableDescriptor, waitForCompletion bool) (*InsertTask, error) {
	config := manager.Config()
	service, ctx, err := GetServiceAndContextForManager(manager)
	if err != nil {
		return nil, err
	}

	insertMethod := config.GetString(fmt.Sprintf("%v.insertMethod", table.Table), InsertMethodLoad)


	return &InsertTask{
		tableDescriptor:   table,
		service:           service,
		context:           ctx,
		manager:           manager,
		insertMethod:      insertMethod,
		waitForCompletion: waitForCompletion,
		projectID:         config.Get(ProjectIDKey),
		datasetID:         config.Get(DataSetIDKey),
	}, nil
}
