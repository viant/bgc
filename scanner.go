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
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type scanner struct {
	columns   []string
	converter toolbox.Converter
	Values    []interface{}
}

func (s *scanner) Columns() ([]string, error) {
	return s.columns, nil
}


func (s *scanner) Scan(destinations ...interface{}) error {

	for i, dest := range destinations {
		value := s.Values[i]
		if dest == nil {
			continue
		}
		err := s.converter.AssignConverted(dest, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func newScaner(config *dsc.Config) *scanner {
	converter := toolbox.NewColumnConverter(config.GetDateLayout())
	return &scanner{converter:*converter,}
}

