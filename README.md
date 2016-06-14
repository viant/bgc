# Datastore Connectivity for BigQuery (bgc)

[![Datastore Connectivity library for BigQuery in Go.](https://goreportcard.com/badge/github.com/viant/bgc)](https://goreportcard.com/report/github.com/viant/bgc)
[![GoDoc](https://godoc.org/github.com/viant/bgc?status.svg)](https://godoc.org/github.com/viant/bgc)

This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#Usage)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)




This library uses SQL mode as default, and streaming API to insert data.
To use legacy SQL please use the following /* USE LEGACY SQL */ hint, in this case you will not be able to fetch repeated and nested fields.
Note that schema defined as part of table descriptor is only required to create a table 
In order to connect to big query this library requires service account id and private key, that has access to project and dataset used.


## Usage:

The following is a very simple example of Reading and Inserting data


```go

package main

import (
    _ 	"github.com/viant/bgc"
)


type MostLikedCity struct {
	City      string
	Visits    int
	Souvenirs []string
}

type  Traveler struct {
	Id            int
	Name          string
	LastVisitTime time.Time
	Achievements  []string
	MostLikedCity MostLikedCity
	VisitedCities []struct {
		City   string
		Visits int
	}
}


func main() {

		config := dsc.NewConfig("bigquery", "", "serviceAccountId:***@developer.gserviceaccount.com,privateKeyPath:/etc/test_service.pem,projectId:spheric-arcadia-98015,datasetId:MyDataset,dateFormat:yyyy-MM-dd hh:mm:ss z")
    	factory := dsc.NewManagerFactory()
    	manager, err := factory.Create(config)
    	if err != nil {
    		t.Fatalf("Failed to create manager %v", err)
    	}
    	// manager := factory.CreateFromURL("file:///etc/myapp/datastore.json")
    	
    	manager.TableDescriptorRegistry().Register(&dsc.TableDescriptor{Table:"travelers3", PkColumns:[]string{"id"}, SchemaUrl:"some_url"})


        traveler := Traveler{}
        success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity FROM travelers WHERE id = ?", []interface{}{4}, nil)
        if err != nil {
            panic(err.Error())
	    }

        var travelers :=  make([]Traveler, 0)
        err:= manager.ReadAll(&interest, "SELECT iid, name, lastVisitTime, visitedCities, achievements, mostLikedCity",nil, nil)
	    if err != nil {
            panic(err.Error())
	    }

        ...
   
        inserted, updated, err := manager.PersistAll(&travelers, "travelers", nil)
        if err != nil {
               panic(err.Error())
   	    }
        ...
        
}
```

## GoCover

[![GoCover](https://gocover.io/github.com/viant/bgc)](https://gocover.io/github.com/viant/bgc)


<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas

**Contributors:**Mikhail Berlyant