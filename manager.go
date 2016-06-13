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
	"database/sql"
	"fmt"
	"reflect"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type config struct {
	*dsc.Config
	email     string
	projectID string
	datasetID string
}

type manager struct {
	*dsc.AbstractManager
	bigQueryConfig *config
}

func (m *manager) PersistAllOnConnection(connection dsc.Connection, dataPointer interface{}, table string, provider dsc.DmlProvider) (inserted int, updated int, err error) {
	toolbox.AssertKind(dataPointer, reflect.Ptr, "dataPointer")
	provider = dsc.NewDmlProviderIfNeeded(provider, table, reflect.TypeOf(dataPointer).Elem())

	insertables, updatables, err := m.ClassifyDataAsInsertableOrUpdatable(connection, dataPointer, table, provider)
	if err != nil {
		return 0, 0, fmt.Errorf("Failed to persist data unable to classify as insertable or updatable %v", err)
	}
	if len(updatables) > 0 {
		return 0, 0, fmt.Errorf("Update is not supproted")
	}

	var rows = make([]map[string]interface{}, 0)
	for _, row := range insertables {
		parametrizerSQL := provider.Get(dsc.SQLTypeInsert, row)
		parser := dsc.NewDmlParser()
		statement, err := parser.Parse(parametrizerSQL.SQL)
		if err != nil {
			return 0, 0, err
		}
		parameters := toolbox.NewSliceIterator(parametrizerSQL.Values)
		valueMap, _ := statement.ColumnValueMap(parameters)
		rows = append(rows, valueMap)
	}

	tableDescriptor := m.TableDescriptorRegistry().Get(table)
	task, err := NewInsertTask(m.Manager, tableDescriptor, true)
	if err != nil {
		return 0, 0, fmt.Errorf("Failed to prepare insert task on %v, due to %v", table, err)
	}
	inserted, err = task.InsertAll(rows)
	if err != nil {
		return 0, 0, fmt.Errorf("Failed to insert data on %v, due to %v", table, err)
	}
	return inserted, 0, nil
}

func (m *manager) ExecuteOnConnection(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {
	parser := dsc.NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	switch statement.Type {
	case "INSERT":
		parameters := toolbox.NewSliceIterator(sqlParameters)
		values, err := statement.ColumnValueMap(parameters)
		if err != nil {
			return nil, fmt.Errorf("Failed to prepare insert data due to %v", err)
		}
		tableDescriptor := m.TableDescriptorRegistry().Get(statement.Table)
		task, err := NewInsertTask(m.Manager, tableDescriptor, true)
		if err != nil {
			return nil, fmt.Errorf("Failed to prepare insert task on %v, due to %v", statement.Table, err)
		}
		err = task.InsertSingle(values)
		if err != nil {
			return nil, fmt.Errorf("Failed to insert data on %v, due to %v", statement.Table, err)
		}
		return dsc.NewSQLResult(int64(1), int64(0)), nil

	default:
		return nil, fmt.Errorf("%v is not supproted by bigquery at m time", statement.Type)
	}
}

func (m *manager) ReadAllOnWithHandlerOnConnection(connection dsc.Connection, sql string, args []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	sql = m.ExpandSQL(sql, args)
	iterator, err := NewQueryIterator(m.Manager, sql)
	if err != nil {
		return fmt.Errorf("Failed to get new query iterator %v %v", sql, err)
	}
	var biqQueryScanner *scanner

	for iterator.HasNext() {
		if biqQueryScanner == nil {
			biqQueryScanner = newScaner(m.Config())
			columns, err := iterator.GetColumns()
			if err != nil {
				return fmt.Errorf("Failed to read bigquery %v - unable to read query schema due to:\n\t%v", sql, err)
			}
			biqQueryScanner.columns = columns
		}
		values, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("Failed to read bigquery %v - unable to fetch values due to:\n\t%v", sql, err)
		}
		biqQueryScanner.Values = values
		var scanner dsc.Scanner = biqQueryScanner
		toContinue, err := readingHandler(scanner)

		if err != nil {
			return fmt.Errorf("Failed to read bigquery %v - unable to map recrod %v", sql, err)
		}
		if !toContinue {
			break
		}
	}
	return nil
}

func newConfig(cfg *dsc.Config) *config {
	return &config{
		Config:    cfg,
		projectID: cfg.Get("projectId"),
		datasetID: cfg.Get("datasetId"),
	}
}
