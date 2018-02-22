package bgc_test

//Refactoring ....
//import (
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//	_ "github.com/viant/bgc"
//	"github.com/viant/dsc"
//	"github.com/viant/dsunit"
//)
//
//func GetManager(t *testing.T) dsc.Manager {
//	keyPath := dsunit.ExpandTestProtocolAsPathIfNeeded("test:///test/test_service.pem")
//	config := dsc.NewConfig("bigquery", "",
//		"serviceAccountId:565950306583-hu98foqgnunu6a1a043plvl03ip60j5g@developer.gserviceaccount.com,privateKeyPath:"+keyPath+",projectId:spheric-arcadia-98015,datasetId:MyDataset,dateFormat:yyyy-MM-dd hh:mm:ss z")
//	factory := dsc.NewManagerFactory()
//	manager, err := factory.Create(config)
//
//	if err != nil {
//		t.Fatalf("Failed to create manager %v", err)
//	}
//	manager.TableDescriptorRegistry().Register(&dsc.TableDescriptor{Table: "travelers3", PkColumns: []string{"id"}, SchemaUrl: "some_url"})
//	manager.TableDescriptorRegistry().Register(&dsc.TableDescriptor{Table: "travelers4", PkColumns: []string{"id"}, SchemaUrl: "some_url"})
//	manager.TableDescriptorRegistry().Register(&dsc.TableDescriptor{Table: "travelers5", PkColumns: []string{"id"}, SchemaUrl: "some_url"})
//	manager.TableDescriptorRegistry().Register(&dsc.TableDescriptor{Table: "abc", PkColumns: []string{"id"}, SchemaUrl: "some_url"})
//	return manager
//}
//
//type MostLikedCity struct {
//	City      string   `column:"city"`
//	Visits    int      `column:"visits"`
//	Souvenirs []string `column:"souvenirs"`
//}
//
//type Traveler struct {
//	Id            int           `column:"id"`
//	Name          string        `column:"name"`
//	LastVisitTime time.Time     `column:"lastVisitTime"`
//	Achievements  []string      `column:"achievements"`
//	MostLikedCity MostLikedCity `column:"mostLikedCity"`
//	VisitedCities []struct {
//		City   string `column:"city"`
//		Visits int    `column:"visits"`
//	}
//}
//
//func TestReadSingle(t *testing.T) {
//
//	dsunit.InitDatastoreFromURL(t, "test://test/init.json")
//	dsunit.PrepareDatastoreFor(t, "MyDataset", "test://test/", "ReadSingle")
//
//	manager := GetManager(t)
//	traveler := Traveler{}
//	success, err := manager.ReadSingle(&traveler, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity FROM travelers1 WHERE id = ?", []interface{}{4}, nil)
//	assert.Nil(t, err)
//	assert.True(t, success)
//	if !success {
//		t.FailNow()
//	}
//	assert.Equal(t, 4, traveler.Id)
//	assert.Equal(t, "Vudi", traveler.Name)
//	assert.Equal(t, 2, len(traveler.VisitedCities))
//
//	assert.Equal(t, "Paris", traveler.VisitedCities[0].City)
//	assert.Equal(t, 1, traveler.VisitedCities[0].Visits)
//
//}
//
//func TestReadAll(t *testing.T) {
//	dsunit.InitDatastoreFromURL(t, "test://test/init.json")
//	dsunit.PrepareDatastoreFor(t, "MyDataset", "test://test/", "ReadAll")
//
//	manager := GetManager(t)
//	var travelers = make([]Traveler, 0)
//	err := manager.ReadAll(&travelers, " SELECT id, name, lastVisitTime, visitedCities, achievements, mostLikedCity FROM travelers2 ORDER BY id", nil, nil)
//	assert.Nil(t, err)
//	assert.Equal(t, 3, len(travelers))
//
//	for _, traveler := range travelers {
//		if traveler.Id == 4 {
//			assert.Equal(t, "Vudi", traveler.Name)
//		}
//	}
//
//}
//
//func TestPersistAll(t *testing.T) {
//	dsunit.InitDatastoreFromURL(t, "test://test/init.json")
//
//	manager := GetManager(t)
//	var travelers = make([]Traveler, 2)
//
//	travelers[0] = Traveler{
//		Id:            10,
//		Name:          "Cook",
//		LastVisitTime: time.Now(),
//		Achievements:  []string{"abc", "jhi"},
//		MostLikedCity: MostLikedCity{City: "Cracow", Visits: 4},
//	}
//
//	travelers[1] = Traveler{
//		Id:            20,
//		Name:          "Robin",
//		LastVisitTime: time.Now(),
//		Achievements:  []string{"w", "a"},
//		MostLikedCity: MostLikedCity{"Moscow", 3, []string{"s3", "sN"}},
//	}
//	inserted, updated, err := manager.PersistAll(&travelers, "travelers3", nil)
//	if err != nil {
//		t.Errorf("%v", err)
//		t.FailNow()
//	}
//	assert.Equal(t, 2, inserted)
//	assert.Equal(t, 0, updated)
//
//	dsunit.ExpectDatasetFor(t, "MyDataset", dsunit.FullTableDatasetCheckPolicy, "test://test/", "PersistAll")
//}
//
//func TestExecuteOnConnection(t *testing.T) {
//	manager := GetManager(t)
//	connetion, err := manager.ConnectionProvider().Get()
//	assert.Nil(t, err)
//	defer connetion.Close()
//	result, err := manager.ExecuteOnConnection(connetion, "INSERT INTO travelers4(id, name) VALUES(?, ?)", []interface{}{20, "Traveler20"})
//	assert.Nil(t, err)
//	rowsAdded, err := result.RowsAffected()
//	assert.Nil(t, err)
//	assert.EqualValues(t, 1, rowsAdded)
//
//	//Test error due to unknown table
//	_, err = manager.ExecuteOnConnection(connetion, "INSERT INTO abc(id, name) VALUES(?, ?)", []interface{}{20, "Traveler20"})
//	assert.NotNil(t, err)
//	_, err = manager.ExecuteOnConnection(connetion, "UPDATE abc SET name = ? WHERE id = ?", []interface{}{"Traveler20", 20})
//	assert.NotNil(t, err)
//}
//
//func TestPersistAllOnConnection(t *testing.T) {
//	manager := GetManager(t)
//	connetion, err := manager.ConnectionProvider().Get()
//	assert.Nil(t, err)
//	defer connetion.Close()
//
//	var travelers = make([]Traveler, 2)
//
//	travelers[0] = Traveler{
//		Id:            10,
//		Name:          "Cook",
//		LastVisitTime: time.Now(),
//		Achievements:  []string{"abc", "jhi"},
//		MostLikedCity: MostLikedCity{City: "Cracow", Visits: 4},
//	}
//
//	travelers[1] = Traveler{
//		Id:            20,
//		Name:          "Robin",
//		LastVisitTime: time.Now(),
//		Achievements:  []string{"w", "a"},
//		MostLikedCity: MostLikedCity{"Moscow", 3, []string{"s3", "sN"}},
//	}
//
//	inserted, _, err := manager.PersistAllOnConnection(connetion, &travelers, "travelers5", nil)
//	assert.Nil(t, err)
//	assert.Equal(t, 2, inserted)
//
//	//updated not supported
//	_, _, err = manager.PersistAllOnConnection(connetion, &travelers, "travelers5", nil)
//	assert.NotNil(t, err)
//
//	//Test error due to unknown table
//	_, _, err = manager.PersistAllOnConnection(connetion, &travelers, "travelers5", nil)
//	assert.NotNil(t, err)
//}
//
//func TestCreateFromURL(t *testing.T) {
//	factory := dsc.NewManagerFactory()
//	url := dsunit.ExpandTestProtocolAsURLIfNeeded("test:///test/config/store.json")
//	manager, err := factory.CreateFromURL(url)
//	assert.Nil(t, err)
//	assert.NotNil(t, manager)
//}
