package bgc

/*

Package asc - Aersopike datastore manager factory

This library comes with the BigQuery datastore manager implementing datastore connectivity manager (viant/dsc)

Usage:


import (
    _ "github.com/viant/bgc"
)

{
 	config := dsc.NewConfig("bigquery", "",
		"serviceAccountId:****@developer.gserviceaccount.com,privateKeyPath:/root/key.pem,projectId:projectId,datasetId:MyDataset,dateFormat:yyyy-MM-dd hh:mm:ss z")
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
}

*/
