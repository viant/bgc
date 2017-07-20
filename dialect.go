package bgc

import (
	"encoding/json"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"strconv"
)

const sequenceSQL = "SELECT COUNT(*) AS cnt FROM %v"

type dialect struct{ dsc.DatastoreDialect }

func (d dialect) DropTable(manager dsc.Manager, datastore string, table string) error {
	config := manager.Config()
	service, context, err := GetServiceAndContextForManager(manager)
	if err != nil {
		return err
	}
	err = service.Tables.Delete(config.Get("projectId"), datastore, table).Context(context).Do()
	if err != nil {
		return err
	}
	return nil
}

func (d dialect) GetDatastores(manager dsc.Manager) ([]string, error) {
	config := manager.Config()
	service, context, err := GetServiceAndContextForManager(manager)
	if err != nil {
		return nil, err
	}
	response, err := service.Datasets.List(config.Get("projectId")).Context(context).Do()
	if err != nil {
		return nil, err
	}
	var result = make([]string, 0)
	for _, dataset := range response.Datasets {
		result = append(result, dataset.DatasetReference.DatasetId)
	}
	return result, nil
}

func (d dialect) GetCurrentDatastore(manager dsc.Manager) (string, error) {
	config := manager.Config()
	return config.Get("datasetId"), nil
}

//GetSequence returns sequence value or error for passed in manager and table/sequence
func (d dialect) GetSequence(manager dsc.Manager, name string) (int64, error) {
	var result = make([]interface{}, 0)
	success, err := manager.ReadSingle(&result, fmt.Sprintf(sequenceSQL, name), []interface{}{}, nil)
	if err != nil || !success {
		return 0, err
	}
	count, _ := strconv.ParseInt(result[0].(string), 10, 64)
	seq := count + 1
	return seq, nil
}

func (d dialect) GetTables(manager dsc.Manager, datastore string) ([]string, error) {
	config := manager.Config()
	service, context, err := GetServiceAndContextForManager(manager)
	if err != nil {
		return nil, err
	}

	call := service.Tables.List(config.Get("projectId"), datastore).Context(context)

	pageToken := ""
	var result = make([]string, 0)
	for {
		if manager.Config().Has("maxResults") {
			maxResults := toolbox.AsInt(manager.Config().Get("maxResults"))
			call.MaxResults(int64(maxResults))

		}
		if pageToken != "" {
			call.PageToken(pageToken)
		}
		response, err := call.Do()
		if err != nil {
			return nil, err
		}
		for _, table := range response.Tables {
			result = append(result, table.TableReference.TableId)
		}
		if response.NextPageToken != "" {
			pageToken = response.NextPageToken
		} else {
			break
		}

	}
	return result, nil
}

func buildSchemaFields(fields []map[string]interface{}) ([]*bigquery.TableFieldSchema, error) {
	var result = make([]*bigquery.TableFieldSchema, 0)
	for _, field := range fields {

		schemaField := &bigquery.TableFieldSchema{}
		if value, found := field["mode"]; found {
			schemaField.Mode = toolbox.AsString(value)
		}
		if value, found := field["name"]; found {
			schemaField.Name = toolbox.AsString(value)
		} else {
			return nil, fmt.Errorf("Invalid schema definition missing required field name %v from %v", field, fields)
		}
		if value, found := field["type"]; found {
			schemaField.Type = toolbox.AsString(value)
		} else {
			return nil, fmt.Errorf("Invalid schema definition missing required field type %v from %v", field, fields)
		}
		if value, found := field["fields"]; found {
			var subFields = make([]map[string]interface{}, 0)
			toolbox.ProcessSlice(value, func(item interface{}) bool {
				if mapValue, ok := item.(map[string]interface{}); ok {
					subFields = append(subFields, mapValue)
				}
				return true
			})
			schemaFields, err := buildSchemaFields(subFields)
			if err != nil {
				return nil, err
			}
			schemaField.Fields = schemaFields
		}
		result = append(result, schemaField)
	}
	return result, nil

}

func tableSchema(descriptor *dsc.TableDescriptor) (*bigquery.TableSchema, error) {
	schema := bigquery.TableSchema{}
	if !descriptor.HasSchema() {
		return nil, fmt.Errorf("Schema not defined on table %v", descriptor.Table)
	}
	if len(descriptor.SchemaUrl) > 0 {
		reader, _, err := toolbox.OpenReaderFromURL(descriptor.SchemaUrl)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		err = json.NewDecoder(reader).Decode(&schema)
		if err != nil {
			return nil, fmt.Errorf("Failed to build decode schema for %v due to %v", descriptor.Table, err)
		}
		if schema.Fields == nil || len(schema.Fields) == 0 {
			return nil, fmt.Errorf("Invalid schema - no fields defined on %v", descriptor.Table)
		}
	} else {
		schemaFields, err := buildSchemaFields(descriptor.Schema)
		if err != nil {
			return nil, err
		}
		schema.Fields = schemaFields
	}
	return &schema, nil
}

func (d dialect) CreateTable(manager dsc.Manager, datastore string, tableName string, options string) error {
	config := manager.Config()
	projectID := config.Get("projectId")
	service, context, err := GetServiceAndContextForManager(manager)
	if err != nil {
		return err
	}
	tableReference := &bigquery.TableReference{
		ProjectId: projectID,
		DatasetId: datastore,
		TableId:   tableName,
	}
	descriptor := manager.TableDescriptorRegistry().Get(tableName)
	tableSchema, err := tableSchema(descriptor)
	if err != nil {
		return err
	}

	table := &bigquery.Table{
		TableReference: tableReference,
		Schema:         tableSchema,
	}
	_, err = service.Tables.Insert(config.Get("projectId"), datastore, table).Context(context).Do()
	if err != nil {
		return err
	}
	return nil
}

func (d dialect) CanPersistBatch() bool {
	return true
}

func newDialect() dsc.DatastoreDialect {
	return &dialect{dsc.NewDefaultDialect()}
}
