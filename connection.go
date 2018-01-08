package bgc

import (
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/viant/dsc"
	"github.com/viant/toolbox/url"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/bigquery/v2"
	"os"
)

var bigQueryScope = "https://www.googleapis.com/auth/bigquery"
var bigQueryInsertScope = "https://www.googleapis.com/auth/bigquery.insertdata"

const (
	CredentialsFileKey = "credentialsFile"
	ServiceAccountId   = "serviceAccountIdKey"
	PrivateKey         = "privateKey"
	PrivateKeyPathKey  = "privateKeyPath"
	ProjectIDKey       = "projectId"
	DataSetIDKey       = "datasetId"
)

var servicePointer = (*bigquery.Service)(nil)
var contextPointer = (*context.Context)(nil)

func asService(wrapped interface{}) (*bigquery.Service, error) {
	if result, ok := wrapped.(*bigquery.Service); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf("failed cast as *aerospike.Client: was %v !", wrappedType.Type())
}

func asContext(wrapped interface{}) (*context.Context, error) {
	if result, ok := wrapped.(*context.Context); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf("failed cast as *aerospike.Client: was %v !", wrappedType.Type())
}

type connection struct {
	*dsc.AbstractConnection
	service *bigquery.Service
	context *context.Context
}

func (c *connection) Close() error {
	// We do not want to cache client - every time use new connection
	return nil
}

func (c *connection) CloseNow() error {
	// We do not want to cache client - every time use new connection
	return nil
}

func (c *connection) Begin() error {
	return nil
}

func (c *connection) Unwrap(target interface{}) interface{} {
	if target == servicePointer {
		return c.service
	}
	if target == contextPointer {
		return c.context
	}
	panic(fmt.Sprintf("unsupported target type %v", target))
}

func (c *connection) Commit() error {
	return nil
}

func (c *connection) Rollback() error {
	return nil
}

type connectionProvider struct {
	*dsc.AbstractConnectionProvider
}

func (cp *connectionProvider) newAuthConfigWithCredentialsFile() (*jwt.Config, error) {
	var credentialsFile = cp.Config().Get(CredentialsFileKey)
	resource := url.NewResource(credentialsFile)
	config := &Credential{}
	err := resource.JSONDecode(config)
	if err != nil {
		return nil, err
	}
	return config.AsJwtConfig(bigQueryScope, bigQueryInsertScope), nil
}

func (cp *connectionProvider) newAuthConfig() (*jwt.Config, error) {
	config := cp.Config()
	serviceAccountID := config.Get(ServiceAccountId)
	var privateKey []byte
	if config.Has(PrivateKey) {
		privateKey = []byte(config.Get(PrivateKey))
	} else {
		var err error
		privateKeyPath := config.Get(PrivateKeyPathKey)
		privateKey, err = ioutil.ReadFile(privateKeyPath)
		if err != nil {
			hostname, _ := os.Hostname()
			return nil, fmt.Errorf("failed to create bigquery connection - unable to read private key from path %v:%v,  %v", hostname, privateKeyPath, err)
		}
	}
	authConfig := &jwt.Config{
		Email:      serviceAccountID,
		PrivateKey: privateKey,
		Subject:    serviceAccountID,
		Scopes:     []string{bigQueryScope, bigQueryInsertScope},
		TokenURL:   google.JWTTokenURL,
	}
	return authConfig, nil
}

func (cp *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := cp.ConnectionProvider.Config()
	var err error
	var authConfig *jwt.Config
	if config.Has(CredentialsFileKey) {
		authConfig, err = cp.newAuthConfigWithCredentialsFile()
	} else {
		authConfig, err = cp.newAuthConfig()
	}

	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	oauthClient := oauth2.NewClient(ctx, authConfig.TokenSource(ctx))

	service, err := bigquery.New(oauthClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery connection - unable to create client:%v", err)
	}
	var bigQueryConnection = &connection{service: service, context: &ctx}
	var connection = bigQueryConnection
	var super = dsc.NewAbstractConnection(config, cp.ConnectionProvider.ConnectionPool(), connection)
	bigQueryConnection.AbstractConnection = super
	return connection, nil
}

func newConnectionProvider(config *dsc.Config) dsc.ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	aerospikeConnectionProvider := &connectionProvider{}
	var connectionProvider dsc.ConnectionProvider = aerospikeConnectionProvider
	var super = dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	aerospikeConnectionProvider.AbstractConnectionProvider = super
	aerospikeConnectionProvider.AbstractConnectionProvider.ConnectionProvider = connectionProvider
	return aerospikeConnectionProvider
}
