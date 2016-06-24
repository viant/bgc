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
package bgc_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
)

func GetManager(t *testing.T) dsc.Manager {
	keyPath := "test:///test/test_service.pem"
	config := dsc.NewConfig("bigquery", "",
		"serviceAccountId:565950306583-hu98foqgnunu6a1a043plvl03ip60j5g@developer.gserviceaccount.com,privateKeyPath:"+keyPath+",projectId:spheric-arcadia-98015,datasetId:MyDataset,dateFormat:yyyy-MM-dd hh:mm:ss z")
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)

	if err != nil {
		t.Fatalf("Failed to create manager %v", err)
	}
	manager.TableDescriptorRegistry().Register(&dsc.TableDescriptor{Table: "travelers3", PkColumns: []string{"id"}, SchemaURL: "some_url"})
	return manager
}

type MostLikedCity struct {
	City      string   `column:"city"`
	Visits    int      `column:"visits"`
	Souvenirs []string `column:"souvenirs"`
}

type Traveler struct {
	Id            int           `column:"id"`
	Name          string        `column:"name"`
	LastVisitTime time.Time     `column:"lastVisitTime"`
	Achievements  []string      `column:"achievements"`
	MostLikedCity MostLikedCity `column:"mostLikedCity"`
	VisitedCities []struct {
		City   string `column:"city"`
		Visits int    `column:"visits"`
	}
}

func TestReadSingle(t *testing.T) {

	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.PrepareDatastoreFor(t, "MyDataset", "test://test/", "ReadSingle")

	manager := GetManager(t)
	traveler := Traveler{}
	success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity FROM travelers1 WHERE id = ?", []interface{}{4}, nil)
	assert.Nil(t, err)
	assert.True(t, success)
	if !success {
		t.FailNow()
	}
	assert.Equal(t, 4, traveler.Id)
	assert.Equal(t, "Vudi", traveler.Name)
	assert.Equal(t, 2, len(traveler.VisitedCities))

	assert.Equal(t, "Paris", traveler.VisitedCities[0].City)
	assert.Equal(t, 1, traveler.VisitedCities[0].Visits)

}

func TestReadAll(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.PrepareDatastoreFor(t, "MyDataset", "test://test/", "ReadAll")

	manager := GetManager(t)
	var travelers = make([]Traveler, 0)
	err := manager.ReadAll(&travelers, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity FROM travelers2 ORDER BY id", nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(travelers))

	for _, traveler := range travelers {
		if traveler.Id == 4 {
			assert.Equal(t, "Vudi", traveler.Name)
		}
	}

}

func TestPersistAll(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")

	manager := GetManager(t)
	var travelers = make([]Traveler, 2)

	travelers[0] = Traveler{
		Id:            10,
		Name:          "Cook",
		LastVisitTime: time.Now(),
		Achievements:  []string{"abc", "jhi"},
		MostLikedCity: MostLikedCity{City: "Cracow", Visits: 4},
	}

	travelers[1] = Traveler{
		Id:            20,
		Name:          "Robin",
		LastVisitTime: time.Now(),
		Achievements:  []string{"w", "a"},
		MostLikedCity: MostLikedCity{"Moscow", 3, []string{"s3", "sN"}},
	}
	inserted, updated, err := manager.PersistAll(&travelers, "travelers3", nil)
	if err != nil {
		t.Errorf("%v", err)
		t.FailNow()
	}
	assert.Equal(t, 2, inserted)
	assert.Equal(t, 0, updated)

	dsunit.ExpectDatasetFor(t, "MyDataset", dsunit.FullTableDatasetCheckPolicy, "test://test/", "PersistAll")
}
