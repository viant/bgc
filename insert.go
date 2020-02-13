package bgc

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	InsertMethodStream       = "stream"
	InsertMethodLoad         = "load"
	InsertWaitTimeoutInMsKey = "insertWaitTimeoutInMs"
	InsertIdColumn           = "insertIdColumn"
	StreamBatchCount         = "streamBatchCount" //can not be more than 10000
	jsonFormat               = "NEWLINE_DELIMITED_JSON"
	createIfNeeded           = "CREATE_IF_NEEDED"
	writeAppend              = "WRITE_APPEND"
	InsertMaxRetires         = "insertMaxRetires"
	defaultInsertWaitTime    = 60000
	defaultInsertMaxRetries  = 2
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
	insertIdColumn    string
	attempts          int
	columns           map[string]dsc.Column
	jobReference      *bigquery.JobReference
}

//InsertSingle streams single records into big query.
func (it *InsertTask) InsertSingle(record map[string]interface{}) error {
	_, err := it.InsertAll([]map[string]interface{}{record})
	return err
}

func (it *InsertTask) insertID(record map[string]interface{}) string {
	if it.insertIdColumn != "" {
		id := toolbox.AsString(record[it.insertIdColumn])
		return id
	}
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

func (it *InsertTask) asJSONMap(record interface{}) map[string]bigquery.JsonValue {
	var jsonValues = make(map[string]bigquery.JsonValue)
	for k, v := range toolbox.AsMap(record) {
		val, ok := normalizeValue(v)
		if !ok {
			continue
		}
		if column, ok := it.columns[k]; ok {
			val = it.adjustDataType(column, val)
		}
		jsonValues[k] = val
	}
	return jsonValues
}

func (it *InsertTask) adjustDataType(column dsc.Column, val interface{}) interface{} {
	switch strings.ToLower(column.DatabaseTypeName()) {
	case "[]string":
		if !toolbox.IsSlice(val) {
			text := toolbox.AsString(val)
			sep := getSeparator(text)
			val = strings.Split(strings.TrimSpace(text), sep)
		}
	case "[]integer":
		if !toolbox.IsSlice(val) {
			text := toolbox.AsString(val)
			sep := getSeparator(text)
			items := strings.Split(strings.TrimSpace(text), sep)
			var values = make([]int, 0)
			for _, item := range items {
				values = append(values, toolbox.AsInt(item))
			}
			val = values
		}
	case "[]float":
		if !toolbox.IsSlice(val) {
			text := toolbox.AsString(val)
			sep := getSeparator(text)
			items := strings.Split(strings.TrimSpace(text), sep)
			var values = make([]float64, 0)
			for _, item := range items {
				values = append(values, toolbox.AsFloat(item))
			}
			val = values
		}
	case "bytes":
		bs, ok := val.([]byte)
		if !ok {
			bs = []byte(toolbox.AsString(val))
		}
		val = bs
	case "boolean":
		val = toolbox.AsBoolean(val)
	case "float":
		val = toolbox.AsFloat(val)
	}
	return val
}

func getSeparator(text string) string {
	sep := ","
	if !strings.Contains(text, sep) {
		sep = " "
	}
	return sep
}

func (it *InsertTask) asMap(record interface{}) map[string]interface{} {
	var jsonValues = make(map[string]interface{})
	for k, v := range toolbox.AsMap(record) {
		val, ok := normalizeValue(v)
		if !ok {
			continue
		}
		if column, ok := it.columns[k]; ok {
			val = it.adjustDataType(column, val)
		}
		jsonValues[k] = val
	}
	return jsonValues
}

func (it *InsertTask) buildLoadData(data interface{}) (io.Reader, int, error) {
	compressed := NewCompressed(nil)
	ranger, ok := data.(toolbox.Ranger)

	var count = 0
	if ok {
		if err := ranger.Range(func(item interface{}) (bool, error) {
			count++
			return true, compressed.Append(it.asMap(item))
		}); err != nil {
			return nil, 0, err
		}
	} else if toolbox.IsSlice(data) {
		for _, item := range toolbox.AsSlice(data) {
			if err := compressed.Append(it.asMap(item)); err != nil {
				return nil, 0, err
			}
			count++
		}
	} else {
		return nil, 0, fmt.Errorf("unsupported type: %T\n", data)
	}
	reader, err := compressed.GetAndClose()
	return reader, count, err
}

func (it *InsertTask) normalizeDataType(record map[string]interface{}) {

}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) LoadAll(data interface{}) (int, error) {
	mediaReader, count, err := it.buildLoadData(data)
	if err != nil || count == 0 {
		return count, err
	}

	if err = it.Insert(mediaReader); err != nil {
		return 0, err
	}
	return count, err
}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) Insert(reader io.Reader) error {
	req := &bigquery.Job{
		JobReference: it.jobReference,
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

	call := it.service.Jobs.Insert(it.projectID, req)
	call = call.Media(reader, googleapi.ContentType("application/octet-stream"))
	job, err := call.Do()
	if err != nil {
		jobJSON, _ := toolbox.AsIndentJSONText(req)
		return fmt.Errorf("failed to submit insert job: %v, %v", jobJSON, err)
	}
	//store job reference in case internal server error, you may recommit job with the same id to avoid duplication
	it.jobReference = job.JobReference
	insertWaitTimeMs := it.manager.Config().GetInt(InsertWaitTimeoutInMsKey, defaultInsertWaitTime)
	if insertWaitTimeMs <= 0 {
		return nil
	}
	if _, err = waitForJobCompletion(it.service, it.context, it.projectID, job.JobReference.JobId, insertWaitTimeMs); err == nil {
		//no error reset job referenc
		it.jobReference = nil
	}
	return err
}

func (it *InsertTask) getRowCount() (int, error) {
	record := []int{}
	_, err := it.manager.ReadSingle(&record, "SELECT COUNT(1) FROM "+it.tableDescriptor.Table, nil, nil)
	if err != nil {
		return 0, err
	}
	if len(record) > 0 {
		return record[0], nil
	}
	return 0, nil

}

func (it *InsertTask) streamRows(rows []*bigquery.TableDataInsertAllRequestRows) (*bigquery.TableDataInsertAllResponse, error) {
	var response *bigquery.TableDataInsertAllResponse
	var err error
	for i := 0; i < it.attempts; i++ {
		insertRequest := &bigquery.TableDataInsertAllRequest{}
		insertRequest.Rows = rows
		requestCall := it.service.Tabledata.InsertAll(it.projectID, it.datasetID, it.tableDescriptor.Table, insertRequest)

		if response, err = requestCall.Context(it.context).Do(); isInternalServerError(err) {
			log.Printf("retrying %v", err)
			log.Printf("no insertIdColumn - duplicates expected")
			if i+i >= it.attempts {
				continue
			}
			time.Sleep(time.Duration(1+i) * time.Second)
			continue
		}
		break

	}
	return response, err
}

func toInsertError(insertErrors []*bigquery.TableDataInsertAllResponseInsertErrors) error {
	if insertErrors == nil {
		return nil
	}
	var messages = make([]string, 0)
	for _, insertError := range insertErrors {
		if len(insertError.Errors) > 0 {
			info, _ := toolbox.AsJSONText(insertError.Errors[0])
			messages = append(messages, info)
			break
		}
	}
	if len(messages) > 0 {
		return fmt.Errorf("%s", strings.Join(messages, ","))
	}
	return fmt.Errorf("%v", insertErrors[0])
}

//InsertAll streams all records into big query, returns number records streamed or error.
func (it *InsertTask) StreamAll(data interface{}) (int, error) {
	records := toolbox.AsSlice(data)
	insertWaitTimeMs := it.manager.Config().GetInt(InsertWaitTimeoutInMsKey, defaultInsertWaitTime)
	estimatedRowCount := len(records)
	if insertWaitTimeMs >= 0 {
		if count, err := it.getRowCount(); err == nil {
			estimatedRowCount += count
		}
	}
	streamBatchCount := it.manager.Config().GetInt(StreamBatchCount, 9999)
	i := 0
	batchCount := 0
	streamRowCount := 0
	var err error
	var waitGroup = &sync.WaitGroup{}
	for i < len(records) {
		var rows = make([]*bigquery.TableDataInsertAllRequestRows, 0)
		for ; i < len(records); i++ {
			record := toolbox.AsMap(records[i])
			rows = append(rows, &bigquery.TableDataInsertAllRequestRows{InsertId: it.insertID(record), Json: it.asJSONMap(record)})
			if len(rows) >= streamBatchCount {
				break
			}
		}
		if len(rows) == 0 {
			break
		}
		streamRowCount += len(rows)
		waitGroup.Add(1)
		go func(batchCount int, rows []*bigquery.TableDataInsertAllRequestRows) {
			defer func() {
				waitGroup.Done()
				if err != nil {
					log.Print(err)
				}
			}()
			insertCall, insertError := it.streamRows(rows)
			if insertError == nil {
				insertError = toInsertError(insertCall.InsertErrors)
			}
			if insertError != nil {
				log.Print(insertError)
				err = insertError
				return
			}
		}(batchCount, rows)
		batchCount++
	}
	if batchCount > 0 {
		waitGroup.Wait() //wait for the first batch only
	}
	if err != nil {
		return 0, err
	}
	//for testing purposes waits till data gets available in table, to supress this wait sets config.params.insertWaitTimeoutInMs=-1
	errCheck := it.waitForData(estimatedRowCount)
	if err == nil {
		err = errCheck
	}
	return streamRowCount, err
}

func (it *InsertTask) waitForData(estimatedRowCount int) error {
	insertWaitTimeMs := it.manager.Config().GetInt(InsertWaitTimeoutInMsKey, defaultInsertWaitTime)
	if insertWaitTimeMs <= 0 {
		return nil
	}
	timeout := time.Millisecond * time.Duration(insertWaitTimeMs)
	startTime := time.Now()
	for i := 0; ; i++ {
		count, err := it.getRowCount()
		if err != nil {
			return err
		}
		if count >= estimatedRowCount {
			break
		}
		time.Sleep(2 * time.Second)
		if time.Now().Sub(startTime) >= timeout {
			break
		}
	}
	return nil
}

//InsertAll streams or load all records into big query, returns number records streamed or error.
func (it *InsertTask) InsertAll(data interface{}) (int, error) {
	var count int
	var err error
	for i := 0; i < it.attempts; i++ {
		if count, err = it.insertAll(data); isInternalServerError(err) {
			if i+i >= it.attempts {
				continue
			}
			time.Sleep(time.Duration(i+1) * time.Second)
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
	insertIdColumn := config.GetString(fmt.Sprintf("%v.%v", table.Table, InsertIdColumn), "")
	dialect := dialect{}

	datastore, _ := dialect.GetCurrentDatastore(manager)

	var columns = make(map[string]dsc.Column)

	if dscColumns, err := dialect.GetColumns(manager, datastore, table.Table); err == nil {
		for _, column := range dscColumns {
			columns[column.Name()] = column
		}
	}
	if _, has := columns[insertIdColumn]; !has {
		insertIdColumn = ""
	}
	attempts := config.GetInt(InsertMaxRetires, defaultInsertMaxRetries)
	if attempts == 0 {
		attempts = 1
	}
	return &InsertTask{
		tableDescriptor:   table,
		service:           service,
		attempts:          attempts,
		context:           ctx,
		manager:           manager,
		insertIdColumn:    insertIdColumn,
		insertMethod:      insertMethod,
		waitForCompletion: waitForCompletion,
		projectID:         config.Get(ProjectIDKey),
		datasetID:         config.Get(DataSetIDKey),
		columns:           columns,
	}, nil
}
