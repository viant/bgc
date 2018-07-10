package bgc



import "bytes"
import (
	"compress/gzip"
	"io"
	"github.com/viant/toolbox"
)

//Compressed represent compressed encoded payload
type Compressed struct {
	output *bytes.Buffer
	writer *gzip.Writer
	encoderFactory toolbox.EncoderFactory
}

//Append append data to compressing stream
func (c *Compressed) Append(data map[string]interface{}) error {
	return c.encoderFactory.Create(c.writer).Encode(&data)
}

//GetAndClose returns reader and cloases the stream
func (c *Compressed) GetAndClose() (io.Reader, error) {
	var err error
	if err = c.writer.Flush(); err != nil {
		return nil, err
	}
	if err = c.writer.Close(); err != nil {
		return nil, err
	}
	return bytes.NewReader(c.output.Bytes()), nil
}

//NewCompressed return new compressed struct
func NewCompressed(encoderFactory toolbox.EncoderFactory) *Compressed {
	if encoderFactory == nil {
		encoderFactory = toolbox.NewJSONEncoderFactory()
	}
	var result =  &Compressed{
		encoderFactory:encoderFactory,
		output:new(bytes.Buffer),
	}
	result.writer = gzip.NewWriter(result.output)
	return result
}