package bgc_test

//Refactoring ...
//import (
//	"github.com/stretchr/testify/assert"
//	"github.com/viant/bgc"
//	"github.com/viant/dsc"
//	"github.com/viant/dsunit"
//	"testing"
//)
//
//func TestGetServiceAndContextForManager(t *testing.T) {
//	manager1 := Manager(t)
//	service, context, err := bgc.GetServiceAndContextForManager(manager1)
//	assert.Nil(t, err)
//	assert.NotNil(t, service)
//	assert.NotNil(t, context)
//
//	config := dsc.NewConfig("ndjson", "[url]", "dateFormat:yyyy-MM-dd hh:mm:ss,ext:json,url:"+dsunit.ExpandTestProtocolAsURLIfNeeded("test:///test/"))
//	manager2, err := dsc.NewManagerFactory().Create(config)
//	assert.Nil(t, err)
//	_, _, err = bgc.GetServiceAndContextForManager(manager2)
//	assert.NotNil(t, err)
//}
