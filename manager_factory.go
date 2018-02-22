package bgc

import (
	"fmt"
	"github.com/viant/dsc"
)

type managerFactory struct{}

func (f *managerFactory) Create(config *dsc.Config) (dsc.Manager, error) {
	var connectionProvider = newConnectionProvider(config)
	manager := &manager{}
	var self dsc.Manager = manager
	super := dsc.NewAbstractManager(config, connectionProvider, self)
	manager.AbstractManager = super

	for _, key := range []string{ProjectIDKey, DataSetIDKey} {
		if !config.Has(key) {
			return nil, fmt.Errorf("config.parameters.%v was missing", key)
		}
	}

	manager.bigQueryConfig = newConfig(config)
	return self, nil
}

func (f managerFactory) CreateFromURL(URL string) (dsc.Manager, error) {
	config, err := dsc.NewConfigFromURL(URL)
	if err != nil {
		return nil, err
	}
	return f.Create(config)
}

func newManagerFactory() dsc.ManagerFactory {
	var result dsc.ManagerFactory = &managerFactory{}
	return result
}
