package bgc

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox/secret"
)

const defaultTimeFormat = "yyyy-MM-dd HH:mm:ss z"

type managerFactory struct{}

func (f *managerFactory) configInit(config *dsc.Config) error {
	if config.Credentials != "" {
		secrets := secret.New("", false)
		credentials, err := secrets.GetCredentials(config.Credentials)
		if err != nil {
			return err
		}
		if !config.Has(ProjectIDKey) {
			config.Parameters[ProjectIDKey] = credentials.ProjectID
		}
	}
	if !config.Has(DateFormatKey) {
		config.Parameters[DateFormatKey] = defaultTimeFormat
	}
	return nil
}

func (f *managerFactory) Create(config *dsc.Config) (dsc.Manager, error) {
	var connectionProvider = newConnectionProvider(config)
	manager := &manager{}
	var self dsc.Manager = manager
	super := dsc.NewAbstractManager(config, connectionProvider, self)
	err := f.configInit(config)
	if err != nil {
		return nil, err
	}
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
