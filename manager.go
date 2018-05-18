package bgc

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
	"time"
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

func (m *manager) PersistData(connection dsc.Connection, data []interface{}, table string, keySetter dsc.KeySetter, sqlProvider func(item interface{}) *dsc.ParametrizedSQL) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	tableDescriptor := m.TableDescriptorRegistry().Get(table)
	task, err := NewInsertTask(m.Manager, tableDescriptor, true)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert task on %v, due to %v", table, err)
	}
	var records = []map[string]interface{}{}
	for _, item := range data {
		records = append(records, toolbox.AsMap(item))
	}

	inserted, err := task.InsertAll(records)
	if err != nil {
		return 0, fmt.Errorf("failed to insert records on %v, due to %v", table, err)
	}
	return inserted, nil
}

func (m *manager) PersistAllOnConnection(connection dsc.Connection, dataPointer interface{}, table string, provider dsc.DmlProvider) (inserted int, updated int, err error) {
	toolbox.AssertKind(dataPointer, reflect.Ptr, "dataPointer")
	provider, err = dsc.NewDmlProviderIfNeeded(provider, table, reflect.TypeOf(dataPointer).Elem())
	if err != nil {
		return 0, 0, err
	}

	insertables, updatables, err := m.ClassifyDataAsInsertableOrUpdatable(connection, dataPointer, table, provider)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to persist data unable to classify as insertable or updatable %v", err)
	}
	if len(updatables) > 0 {
		for _, row := range updatables {
			parametrizerSQL := provider.Get(dsc.SQLTypeUpdate, row)
			parser := dsc.NewDmlParser()
			statement, err := parser.Parse(parametrizerSQL.SQL)
			if err != nil {
				return 0, 0, err
			}
			resultset, err := m.Execute(statement.SQL, statement.Values...)
			if err != nil {
				return 0, 0, err
			}
			affected, _ := resultset.RowsAffected()
			updated += int(affected)
		}
	}

	if len(insertables) > 0 {
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
			return 0, 0, fmt.Errorf("failed to prepare insert task on %v, due to %v", table, err)
		}
		inserted, err = task.InsertAll(rows)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to insert data on %v, due to %v", table, err)
		}
	}
	return inserted, updated, nil
}

func (m *manager) runInsert(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {
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
			return nil, fmt.Errorf("failed to prepare insert data due to %v", err)
		}
		tableDescriptor := m.TableDescriptorRegistry().Get(statement.Table)
		task, err := NewInsertTask(m.Manager, tableDescriptor, true)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare insert task on %v, due to %v", statement.Table, err)
		}

		err = task.InsertSingle(values)
		if err != nil {
			return nil, fmt.Errorf("failed to insert data %v, %v", statement.Table, err)
		}
		return dsc.NewSQLResult(int64(1), int64(0)), nil

	default:
		return nil, fmt.Errorf("%v is not supproted by bigquery at m time", statement.Type)
	}
}

func (m *manager) ExecuteOnConnection(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {
	sql = strings.TrimSpace(sql)
	lowerCaseSQL := strings.ToLower(sql)
	if strings.HasPrefix(lowerCaseSQL, "delete") && ! strings.Contains(lowerCaseSQL, "where") {
		sql += " WHERE 1 = 1"
	} else 	if strings.HasPrefix(lowerCaseSQL, "insert") {
		return m.runInsert(connection, sql, sqlParameters)
	}
	service, context, err := GetServiceAndContextForManager(m)
	if err != nil {
		return nil, err
	}
	config := m.Config()
	queryTask := &queryTask{
		service:   service,
		context:   context,
		projectID: config.Get(ProjectIDKey),
		datasetID: config.Get(DataSetIDKey),
	}

	for _, param := range sqlParameters {
		switch value := param.(type) {
		case string:
			sql = strings.Replace(sql, "?", "'"+value+"'", 1)
		case time.Time:
			sql = strings.Replace(sql, "?", "'"+value.String()+"'", 1)
		case *time.Time:
			sql = strings.Replace(sql, "?", "'"+value.String()+"'", 1)
		default:
			sql = strings.Replace(sql, "?", toolbox.AsString(param), 1)
		}
	}
	job, err := queryTask.run(sql)
	if err != nil {
		return nil, err
	}
	if job.Statistics != nil && job.Statistics.Query != nil {
		return dsc.NewSQLResult(job.Statistics.Query.NumDmlAffectedRows, int64(0)), nil
	}
	return dsc.NewSQLResult(int64(0), int64(0)), nil
}

func (m *manager) ReadAllOnWithHandlerOnConnection(connection dsc.Connection, sql string, args []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	sql = m.ExpandSQL(sql, args)
	iterator, err := NewQueryIterator(m.Manager, sql)
	if err != nil {
		return fmt.Errorf("failed to get new query iterator %v %v", sql, err)
	}


	var biqQueryScanner *scanner
	for iterator.HasNext() {
		if biqQueryScanner == nil {
			biqQueryScanner = newScaner(m.Config())
			columns, err := iterator.GetColumns()
			if err != nil {
				return fmt.Errorf("failed to read bigquery %v - unable to read query schema due to:\n\t%v", sql, err)
			}
			biqQueryScanner.columns = columns
		}
		values, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("failed to read bigquery %v - unable to fetch values due to:\n\t%v", sql, err)
		}
		biqQueryScanner.Values = values
		var scanner dsc.Scanner = biqQueryScanner
		toContinue, err := readingHandler(scanner)

		if err != nil {
			return fmt.Errorf("failed to read bigquery %v - unable to map recrod %v", sql, err)
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
		projectID: cfg.Get(ProjectIDKey),
		datasetID: cfg.Get(DataSetIDKey),
	}
}
