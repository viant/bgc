# Datastore Connectivity for BigQuery (bgc)

[![Datastore Connectivity library for BigQuery in Go.](https://goreportcard.com/badge/github.com/viant/bgc)](https://goreportcard.com/report/github.com/viant/bgc)
[![GoDoc](https://godoc.org/github.com/viant/bgc?status.svg)](https://godoc.org/github.com/viant/bgc)

This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#Usage)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)




This library uses SQL mode and streaming API to insert data as default.
To use legacy SQL please use the following /* USE LEGACY SQL */ hint, in this case you will not be able to fetch repeated and nested fields.
To control insert method just provide config.parameters with the following value:
    
    _**table_name**_.insertMethod = "load"

Note that if streaming is used, currently UPDATE and DELETE statements are not supported.


## Credentials

1. Google secrets for service account

a) credential can be a name with extension of the JSON secret file placed into ~/.secret/ folder

config.yaml
```yaml
driverName: bigquery
credentials: bq # place your big query secret json to ~/.secret/bg.json
parameters:
  datasetId: myDataset
```

b) full URL to secret file

config.yaml
```yaml
driverName: bigquery
credentials: file://tmp/secret/mySecret.json
parameters:
  datasetId: myDataset
```

[Secret file](https://github.com/viant/toolbox/blob/master/cred/config.go) has to specify the following attributes:

````json
{
	//google cloud credential
	ClientEmail  string `json:"client_email,omitempty"`
	TokenURL     string `json:"token_uri,omitempty"`
	PrivateKey   string `json:"private_key,omitempty"`
	PrivateKeyID string `json:"private_key_id,omitempty"`
	ProjectID  string `json:"project_id,omitempty"`
}
````


2. Private key (pem)


config.yaml
```yaml
driverName: bigquery
credentials: bq # place your big query secret json to ~/.secret/bg.json
parameters:
  serviceAccountId: "***@developer.gserviceaccount.com"
  datasetId: MyDataset
  projectId: spheric-arcadia-98015
  privateKeyPath: /tmp/secret/bq.pem
```




## Usage:

The following is a very simple example of Reading and Inserting data


```go

package main

import (
    _ 	"github.com/viant/bgc"
    "github.com/viant/dsc"
    "time"    
    "log"
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

    config, err := dsc.NewConfigWithParameters("bigquery", "",
    	    "bq", // google cloud secret placed in ~/.secret/bg.json
            map[string]string{
                "datasetId":"MyDataset",
            })

    if err != nil {
        log.Fatal(err)
    }

		
    factory := dsc.NewManagerFactory()
    manager, err := factory.Create(config)
    if err != nil {
        log.Fatalf("Failed to create manager %v", err)
    }
   

    traveler := Traveler{}
    success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity FROM travelers WHERE id = ?", []interface{}{4}, nil)
    if err != nil {
        panic(err.Error())
    }

    travelers :=  make([]Traveler, 0)
    err:= manager.ReadAll(&interest, "SELECT iid, name, lastVisitTime, visitedCities, achievements, mostLikedCity",nil, nil)
    if err != nil {
        panic(err.Error())
    }

   // ...

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