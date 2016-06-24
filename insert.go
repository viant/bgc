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

//Package bgc - BigQuery streaming task
package bgc

import (
	"fmt"
	"log"
	"time"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
)

var pullDuration = 5 * time.Second
var streamingTimeout = 360 * time.Second

//InsertTask represents insert streaming task.
type InsertTask struct {
	tableDescriptor   *dsc.TableDescriptor
	service           *bigquery.Service
	context           context.Context
	projectID         string
	datasetID         string
	waitForCompletion bool
	manager           dsc.Manager
}

//InsertSingle streams single rows into big query.
func (it *InsertTask) InsertSingle(row map[string]interface{}) error {
	_, err := it.InsertAll([]map[string]interface{}{row})
	return err
}

func (it *InsertTask) insertID(row map[string]interface{}) string {
	pkValue := ""
	for _, pkColumn := range it.tableDescriptor.PkColumns {
		pkValue = pkValue + toolbox.AsString(row[pkColumn])
	}
	return pkValue
}

func (it *InsertTask) jsonValues(row map[string]interface{}) map[string]bigquery.JsonValue {
	var jsonValues = make(map[string]bigquery.JsonValue)
	for k, v := range row {
		jsonValues[k] = v
	}
	return jsonValues
}

func (it *InsertTask) getTableRowCount() (int, error) {
	iterator, err := NewQueryIterator(it.manager, "SELECT COUNT(*) AS cnt FROM "+it.tableDescriptor.Table)
	if err != nil {
		return 0, err
	}
	if iterator.HasNext() {
		row, err := iterator.Next()
		if err != nil {
			return 0, err
		}
		return toolbox.AsInt(toolbox.AsString(row[0])), nil
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

	return fmt.Errorf("Timeout - unable to check streaming data in big query")
}

//InsertAll streams all rows into big query, returns number rows streamed or error.
func (it *InsertTask) InsertAll(rows []map[string]interface{}) (int, error) {
	insertRequst := &bigquery.TableDataInsertAllRequest{}
	var insertRequestRows = make([]*bigquery.TableDataInsertAllRequestRows, len(rows))
	for i, row := range rows {
		insertRequestRows[i] = &bigquery.TableDataInsertAllRequestRows{InsertId: it.insertID(row), Json: it.jsonValues(row)}
	}
	insertRequst.Rows = insertRequestRows
	streamRowCount := len(insertRequestRows)
	var initialRowCount = 0
	if it.waitForCompletion {
		var err error
		initialRowCount, err = it.getTableRowCount()
		if err != nil {
			return 0, err
		}
	}
	requestCall := it.service.Tabledata.InsertAll(it.projectID, it.datasetID, it.tableDescriptor.Table, insertRequst)
	response, err := requestCall.Context(it.context).Do()
	if err != nil {
		return 0, err
	}
	if response.InsertErrors != nil && len(response.InsertErrors) > 0 {
		return 0, fmt.Errorf("Failed to inser rows %v", response.InsertErrors[0].Errors[0])
	}
	if it.waitForCompletion {
		err := it.waitForInsertCompletion(streamRowCount, initialRowCount)
		if err != nil {
			return streamRowCount, err
		}
	}
	return streamRowCount, nil
}

//NewInsertTask creates a new streaming insert task, it takes manager, table descript with schema, waitForCompletion flag with time duration.
func NewInsertTask(manager dsc.Manager, tableDescriptor *dsc.TableDescriptor, waitForCompletion bool) (*InsertTask, error) {
	config := manager.Config()
	service, context, err := GetServiceAndContextForManager(manager)
	if err != nil {
		return nil, err
	}
	return &InsertTask{
		tableDescriptor:   tableDescriptor,
		service:           service,
		context:           context,
		manager:           manager,
		waitForCompletion: waitForCompletion,
		projectID:         config.Get("projectId"),
		datasetID:         config.Get("datasetId"),
	}, nil
}
