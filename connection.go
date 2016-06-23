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
package bgc

import (
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/viant/dsc"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/bigquery/v2"
)

var bigQueryScope = "https://www.googleapis.com/auth/bigquery"
var bigQueryInsertScope = "https://www.googleapis.com/auth/bigquery.insertdata"

var servicePointer = (*bigquery.Service)(nil)
var contextPointer = (*context.Context)(nil)

func asService(wrapped interface{}) (*bigquery.Service, error) {
	if result, ok := wrapped.(*bigquery.Service); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf("Failed cast as *aerospike.Client: was %v !", wrappedType.Type())
}

func asContext(wrapped interface{}) (*context.Context, error) {
	if result, ok := wrapped.(*context.Context); ok {
		return result, nil
	}
	wrappedType := reflect.ValueOf(wrapped)
	return nil, fmt.Errorf("Failed cast as *aerospike.Client: was %v !", wrappedType.Type())
}

type connection struct {
	dsc.AbstractConnection
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
	panic(fmt.Sprintf("Unsupported target type %v", target))
}

func (c *connection) Commit() error {
	return nil
}

func (c *connection) Rollback() error {
	return nil
}

type connectionProvider struct {
	dsc.AbstractConnectionProvider
}

func (cp *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := cp.ConnectionProvider.Config()
	serviceAccountID := config.Get("serviceAccountId")
	var privateKey []byte
	if config.Has("privateKey") {
		privateKey = []byte(config.Get("privateKey"))
	} else {
		var err error
		privateKeyPath := config.Get("privateKeyPath")
		privateKey, err = ioutil.ReadFile(privateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("Failed to create bigquery connection - unable to read private key from path %v, %v", privateKeyPath, err)
		}

	}

	authConfig := jwt.Config{
		Email:      serviceAccountID,
		PrivateKey: privateKey,
		Subject:    serviceAccountID,
		Scopes:     []string{bigQueryScope, bigQueryInsertScope},
		TokenURL:   google.JWTTokenURL,
	}

	context := context.Background()
	oauthClient := oauth2.NewClient(context, authConfig.TokenSource(context))
	service, err := bigquery.New(oauthClient)
	if err != nil {
		return nil, fmt.Errorf("Failed to create bigquery connection - unable to create client:%v", err)
	}
	var bigQueryConnection = &connection{service: service, context: &context}
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
