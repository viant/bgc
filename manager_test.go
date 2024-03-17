package bgc_test

import (
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"log"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"
)

var inited int32 = 0

func initDb(t *testing.T) bool {

	if !toolbox.FileExists(path.Join(os.Getenv("HOME"), ".secret/viant-e2e.json")) {
		return false
	}

	if atomic.LoadInt32(&inited) == 1 {
		return true
	}
	result := dsunit.InitFromURL(t, "test/init.yaml")
	atomic.StoreInt32(&inited, 1)
	return result

}

func GetManager(t *testing.T) dsc.Manager {

	config, err := dsc.NewConfigFromURL("test/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if err != nil {
		t.Fatal(err)
	}
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

	if !initDb(t) {
		return
	}

	if !dsunit.PrepareFor(t, "myDataset", "test/data", "ReadSingle") {
		return
	}
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
	if !initDb(t) {
		return
	}

	if !dsunit.PrepareFor(t, "myDataset", "test/data", "ReadAll") {
		return
	}

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
	if !initDb(t) {
		return
	}
	manager := GetManager(t)
	table := manager.TableDescriptorRegistry().Get("travelers3")
	if !assert.NotNil(t, table) {
		return
	}
	table.PkColumns = []string{"id"}
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
	dsunit.ExpectFor(t, "myDataset", dsunit.FullTableDatasetCheckPolicy, "test/data", "PersistAll")
}

func TestExecuteOnConnection(t *testing.T) {
	if !initDb(t) {
		return
	}
	manager := GetManager(t)
	config := manager.Config()
	config.Parameters["travelers4.insertMethod"] = "load"
	connetion, err := manager.ConnectionProvider().Get()
	assert.Nil(t, err)
	defer connetion.Close()
	result, err := manager.ExecuteOnConnection(connetion, "INSERT INTO travelers4(id, name) VALUES(?, ?)", []interface{}{20, "Traveler20"})
	assert.Nil(t, err)
	rowsAdded, err := result.RowsAffected()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, rowsAdded)

	//Test error due to unknown table
	_, err = manager.ExecuteOnConnection(connetion, "INSERT INTO travelers4(id, name) VALUES(?, ?)", []interface{}{20, "Traveler20"})
	assert.Nil(t, err)
	//Test error due to unknown table
	_, err = manager.ExecuteOnConnection(connetion, "INSERT INTO travelers4(id, name) VALUES(?, ?)", []interface{}{21, "Traveler21"})
	assert.Nil(t, err)

	_, err = manager.ExecuteOnConnection(connetion, "UPDATE travelers4 SET name = ? WHERE id = ?", []interface{}{"Traveler 20", 20})
	assert.Nil(t, err)
	dsunit.ExpectFor(t, "myDataset", dsunit.FullTableDatasetCheckPolicy, "test/data", "Execute")

}

func TestPersistAllOnConnection(t *testing.T) {
	if !initDb(t) {
		return
	}
	manager := GetManager(t)
	connection, err := manager.ConnectionProvider().Get()
	assert.Nil(t, err)
	defer connection.Close()

	table := manager.TableDescriptorRegistry().Get("travelers5")
	if !assert.NotNil(t, table) {
		return
	}
	table.PkColumns = []string{"id"}

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

	inserted, _, err := manager.PersistAllOnConnection(connection, &travelers, "travelers5", nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, inserted)

	//updated not supported
	_, _, err = manager.PersistAllOnConnection(connection, &travelers, "travelers5", nil)
	assert.NotNil(t, err)

	//Test error due to unknown table
	_, _, err = manager.PersistAllOnConnection(connection, &travelers, "travelers5", nil)
	assert.NotNil(t, err)
}
