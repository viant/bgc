package bgc

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"io"
	"strings"
	"time"
)

const (
	InsertMethodStream       = "stream"
	InsertMethodLoad         = "load"
	InsertWaitTimeoutInMsKey = "insertWaitTimeoutInMs"
	jsonFormat               = "NEWLINE_DELIMITED_JSON"
	createIfNeeded           = "CREATE_IF_NEEDED"
	writeAppend              = "WRITE_APPEND"
)

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

	if toolbox.IsTime(value) {
		ts := toolbox.AsTime(value, "")
		return value, ts != nil
	} else if toolbox.IsStruct(value) {
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

func asJSONMap(record interface{}) map[string]bigquery.JsonValue  {
	var jsonValues = make(map[string]bigquery.JsonValue )
	for k, v := range toolbox.AsMap(record) {
		val, ok := normalizeValue(v)
		if !ok {
			continue
		}
		jsonValues[k] = val
	}
	return jsonValues
}

func asMap(record interface{}) map[string]interface{} {
	var jsonValues = make(map[string]interface{})
	for k, v := range toolbox.AsMap(record) {
		val, ok := normalizeValue(v)
		if !ok {
			continue
		}
		jsonValues[k] = val
	}
	return jsonValues
}

func buildRecord(record map[string]interface{}) map[string]interface{} {
	result := data.NewMap()
	for k, v := range toolbox.AsMap(record) {
		result.SetValue(k, v)
	}
	return toolbox.DeleteEmptyKeys(result)
}

func (it *InsertTask) buildLoadData(data interface{}) (io.Reader, int, error) {
	compressed := NewCompressed(nil)
	ranger, ok := data.(toolbox.Ranger);
	var count = 0
	if ok {
		if err := ranger.Range(func(item interface{}) (bool, error) {
			return true, compressed.Append(asMap(item))
		});err != nil {
			return nil, 0, err
		}
	} else {
		for _, item := range toolbox.AsSlice(data) {
			if err := compressed.Append(asMap(item));err != nil {
				return nil, 0, err
			}
			count++
		}
	}
	reader, err := compressed.GetAndClose()
	return reader, count, err
}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) LoadAll(data interface{}) (int, error) {
	mediaReader, count, err := it.buildLoadData(data)
	if err != nil {
		return count, err
	}
	if err = it.Insert(mediaReader);err  != nil {
		return 0, err
	}
	return count, err
}


//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) Insert(reader io.Reader) error {
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
	call = call.Media(reader, googleapi.ContentType("application/octet-stream"))
	job, err := call.Do()
	if err != nil {
		return err
	}
	insertWaitTimeMs := it.manager.Config().GetInt(InsertWaitTimeoutInMsKey, 60000)
	_, err = waitForJobCompletion(it.service, it.context, it.projectID, job.JobReference.JobId, insertWaitTimeMs)
	return err
}



//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) StreamAll(data interface{}) (int, error) {
	insertRequest := &bigquery.TableDataInsertAllRequest{}
	records := toolbox.AsSlice(data)
	var insertRequestRows = make([]*bigquery.TableDataInsertAllRequestRows, len(records))
	for i, record := range records {
		record := buildRecord(toolbox.AsMap(record))
		insertRequestRows[i] = &bigquery.TableDataInsertAllRequestRows{InsertId: it.insertID(record), Json: asJSONMap(record)}
	}
	insertRequest.Rows = insertRequestRows
	streamRowCount := len(insertRequestRows)
	requestCall := it.service.Tabledata.InsertAll(it.projectID, it.datasetID, it.tableDescriptor.Table, insertRequest)
	response, err := requestCall.Context(it.context).Do()
	if err != nil {
		return 0, err
	}
	if len(response.InsertErrors) > 0 {
		for _, insertError := range response.InsertErrors {
			if len(insertError.Errors) > 0 {
				return streamRowCount, fmt.Errorf(insertError.Errors[0].Reason + " " + insertError.Errors[0].Message)
			}
		}
		return streamRowCount, fmt.Errorf("unknown error: %v", response)
	}
	err = it.waitForEmptyStreamingBuffer()
	return streamRowCount, err
}



func (it *InsertTask) waitForEmptyStreamingBuffer() error {
	insertWaitTimeMs := it.manager.Config().GetInt(InsertWaitTimeoutInMsKey, 60000)
	waitSoFarMs := 0
	for i := 0; ; i++ {
		tableRequest := it.service.Tables.Get(it.projectID, it.datasetID, it.tableDescriptor.Table)
		table, err := tableRequest.Context(it.context).Do()
		if err != nil {
			return err
		}
		if table.StreamingBuffer == nil || table.StreamingBuffer.EstimatedRows == 0 {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(tickInterval*(1+i%20)))
		waitSoFarMs += tickInterval
		if waitSoFarMs > insertWaitTimeMs {
			break
		}
	}
	return nil
}

//InsertAll streams or load all records into big query, returns number records streamed or error.
func (it *InsertTask) InsertAll(data interface{}) (int, error) {
	var count int
	var err error
	var retrySleepMs = 2000
	for i := 0; i < 3; i++ {
		count, err = it.insertAll(data)
		if err != nil && strings.Contains(err.Error(), "Error 503") {
			time.Sleep(time.Duration(retrySleepMs*(1+i)) * time.Millisecond)
			continue
		}
		break
	}
	return count, err
}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) insertAll(records interface{}) (int, error) {
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

