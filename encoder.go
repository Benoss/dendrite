package dendrite

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
)

type Encoder interface {
	Encode(out map[string]Column, writer io.Writer)
}

type JsonEncoder struct{}
type GelfEncoder struct{}
type StatsdEncoder struct{}
type RawStringEncoder struct{}

func NewEncoder(u *url.URL) (Encoder, error) {
	a := strings.Split(u.Scheme, "+")
	switch a[len(a)-1] {
	case "json":
		return new(JsonEncoder), nil
	case "statsd":
		return new(StatsdEncoder), nil
	case "gelf":
		return new(GelfEncoder), nil
	}
	return new(RawStringEncoder), nil
}

func (*RawStringEncoder) Encode(out map[string]Column, writer io.Writer) {
	for _, v := range out {
		if v.Type == String {
			writer.Write([]byte(v.Value.(string) + "\n"))
		}
	}
}

func (*GelfEncoder) Encode(out map[string]Column, writer io.Writer) {
	stripped := make(map[string]interface{})
	for k, v := range out {
		switch k {
		case "_hostname":
			stripped["host"] = v.Value
		case "_group":
			stripped["_config-name"] = v.Value
		case "_file":
			stripped["_file"] = v.Value
		case "_time":
			stripped["timestamp"] = v.Value
		case "_offset":
			stripped["_offset"] = v.Value
		case "message":
			stripped["short_message"] = v.Value
		case "short_message":
			stripped["short_message"] = v.Value
		case "full_message":
			stripped["full_message"] = v.Value
		case "level":
			stripped["level"] = v.Value
		default:
			stripped["_"+k] = v.Value
		}
	}

	stripped["version"] = "1.1"

	bytes, err := json.Marshal(stripped)
	if err != nil {
		panic(err)
	}
	writer.Write(bytes)
}

func (*JsonEncoder) Encode(out map[string]Column, writer io.Writer) {
	stripped := make(map[string]interface{})
	for k, v := range out {
		stripped[k] = v.Value
	}
	bytes, err := json.Marshal(stripped)
	if err != nil {
		panic(err)
	}
	bytes = append(bytes, '\n')
	writer.Write(bytes)
}

func (*StatsdEncoder) Encode(out map[string]Column, writer io.Writer) {
	for k, v := range out {
		switch v.Type {
		case Gauge:
			writer.Write([]byte(fmt.Sprintf("%s:%d|g", k, v.Value)))
		case Metric:
			writer.Write([]byte(fmt.Sprintf("%s:%d|m", k, v.Value)))
		case Counter:
			writer.Write([]byte(fmt.Sprintf("%s:%d|c", k, v.Value)))
		}
	}
}
