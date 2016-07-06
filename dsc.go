package bgc

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("bigquery", newManagerFactory())
	dsc.RegisterDatastoreDialectable("bigquery", newDialect())
}

func init() {
	register()
}
