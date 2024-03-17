package bgc

import (
	"fmt"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/gcp/client"
	"github.com/viant/scy/cred/secret"
	"net/http"

	"github.com/viant/dsc"

	"golang.org/x/net/context"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"reflect"
)

var userAgent = "bgc/0.5.0"
var bigQueryScope = "https://www.googleapis.com/auth/bigquery"
var bigQueryInsertScope = "https://www.googleapis.com/auth/bigquery.insertdata"

var googleDriveReadOnlyScope = "https://www.googleapis.com/auth/drive.readonly"
var prodAddr = "https://www.googleapis.com/bigquery/v2/"

const (
	ServiceAccountIdKey = "serviceAccountId"
	PrivateKey          = "privateKey"
	PrivateKeyPathKey   = "privateKeyPath"
	ProjectIDKey        = "projectId"
	DataSetIDKey        = "datasetId"
	DateFormatKey       = "dateFormat"
	MaxResultsKey       = "maxResults"
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
	service   *bigquery.Service
	projectID string
	context   *context.Context
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

func (cp *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := cp.ConnectionProvider.Config()
	var err error

	ctx := context.Background()
	var result = &connection{context: &ctx}

	sec := secret.New()
	var options = make([]option.ClientOption, 0)
	options = append(options, option.WithScopes(bigQueryScope, bigQueryInsertScope, googleDriveReadOnlyScope))
	usesAuth := false
	if config.Credentials != "" {
		aSecret, err := sec.Lookup(context.Background(), secret.Resource(config.Credentials))
		if err != nil {
			return nil, err
		}
		data := aSecret.String()
		options = append(options, option.WithCredentialsJSON([]byte(data)))
		options = append(options, option.WithUserAgent(userAgent))

		usesAuth = true
	}

	if !usesAuth {
		gcpService := gcp.New(client.NewScy())
		httpClient, err := gcpService.AuthClient(context.Background(), append(gcp.Scopes, bigQueryScope, bigQueryInsertScope, googleDriveReadOnlyScope)...)
		if err == nil && httpClient != nil {
			options = append(options, option.WithHTTPClient(httpClient))
		}

	}

	service, err := bigquery.NewService(ctx, options...)
	result.service = service
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery connection - unable to create client:%v", err)
	}
	var connection = result
	var super = dsc.NewAbstractConnection(config, cp.ConnectionProvider.ConnectionPool(), connection)
	result.AbstractConnection = super
	return connection, nil
}

func getDefaultClient(ctx context.Context) (*http.Client, error) {
	o := []option.ClientOption{
		option.WithEndpoint(prodAddr),
		option.WithScopes(bigQueryScope, bigQueryInsertScope, googleDriveReadOnlyScope),
		option.WithUserAgent(userAgent),
	}
	httpClient, _, err := htransport.NewClient(ctx, o...)
	return httpClient, err
}

func newConnectionProvider(config *dsc.Config) dsc.ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	provider := &connectionProvider{}
	var connectionProvider dsc.ConnectionProvider = provider
	var super = dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	provider.AbstractConnectionProvider = super
	provider.AbstractConnectionProvider.ConnectionProvider = connectionProvider
	return provider
}
