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
	if len(destinations) == 1 {
		if aMap, ok := destinations[0].(map[string]interface{}); ok {
			for i, column := range s.columns {
				aMap[column] = s.Values[i]
			}
			return nil
		}
		if aMap, ok := destinations[0].(*map[string]interface{}); ok {
			for i, column := range s.columns {
				(*aMap)[column] = s.Values[i]
			}
			return nil
		}

	}
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
	return &scanner{converter: *converter}
}
