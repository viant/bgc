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
	return dsc.NewScanner(&scanner{converter: *converter})
}
