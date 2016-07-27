package bgc

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("bigquery", newManagerFactory())
	dsc.RegisterDatastoreDialect("bigquery", newDialect())
}

func init() {
	register()
}
