package bgc_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"testing"
)

func Manager(t *testing.T) dsc.Manager {
	keyPath := dsunit.ExpandTestProtocolAsPathIfNeeded("test:///test/test_service.pem")
	config := dsc.NewConfig("bigquery", "",
		"serviceAccountId:565950306583-hu98foqgnunu6a1a043plvl03ip60j5g@developer.gserviceaccount.com,privateKeyPath:"+keyPath+",projectId:spheric-arcadia-98015,datasetId:MyDataset,dateFormat:yyyy-MM-dd hh:mm:ss z")
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	assert.Nil(t, err)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	return manager
}

func TestGetDatastores(t *testing.T) {
	manager := Manager(t)
	dialect := dsc.GetDatastoreDialect("bigquery")
	datastores, err := dialect.GetDatastores(manager)
	assert.Nil(t, err)
	assert.True(t, len(datastores) > 0)

}

func TestGetDatastore(t *testing.T) {
	manager := Manager(t)
	dialect := dsc.GetDatastoreDialect("bigquery")
	datastore, err := dialect.GetCurrentDatastore(manager)
	assert.Nil(t, err)
	assert.Equal(t, "MyDataset", datastore)

}

func TestGetTables(t *testing.T) {
	{
		manager := Manager(t)
		dialect := dsc.GetDatastoreDialect("bigquery")
		_, err := dialect.GetTables(manager, "MyDataset")
		assert.Nil(t, err)
	}

	{
		manager := Manager(t)
		dialect := dsc.GetDatastoreDialect("bigquery")
		_, err := dialect.GetTables(manager, "Fake")
		assert.NotNil(t, err)
	}

}
