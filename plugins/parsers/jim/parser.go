package jim

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	iotm "github.com/alivinco/iotmsglibgo"
	"github.com/influxdata/telegraf"
)

type JimParser struct {
	MetricName  string
	TagKeys     []string
	DefaultTags map[string]string
}
func (p *JimParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	config := map[string]string{"override_payload_type":"jim0"}
	iotMsg , err := iotm.ConvertBytesToIotMsg("",buf,config)

	metrics := make([]telegraf.Metric, 0)

	//var jsonOut map[string]interface{}
	//err := json.Unmarshal(buf, &jsonOut)
	//if err != nil {
	//	err = fmt.Errorf("unable to parse out as JSON, %s", err)
	//	return nil, err
	//}

	tags := make(map[string]string)
	tags["type"] = iotMsg.Type
	tags["cls"] = iotMsg.Class
	tags["subcls"] = iotMsg.SubClass
	//for k, v := range p.DefaultTags {
	//	tags[k] = v
	//}
	//
	//for _, tag := range p.TagKeys {
	//	switch v := jsonOut[tag].(type) {
	//	case string:
	//		tags[tag] = v
	//	}
	//	delete(jsonOut, tag)
	//}
	//
	//f := JSONFlattener{}
	//err = f.FlattenJSON("", jsonOut)
	//if err != nil {
	//	return nil, err
	//}
	f := make(map[string]interface{})
	f["def_num"] =  iotMsg.GetDefaultInt()
	metric, err := telegraf.NewMetric(p.MetricName, tags, f, time.Now().UTC())

	if err != nil {
		return nil, err
	}
	return append(metrics, metric), nil
}

func (p *JimParser) ParseLine(line string) (telegraf.Metric, error) {
	metrics, err := p.Parse([]byte(line + "\n"),"")

	if err != nil {
		return nil, err
	}

	if len(metrics) < 1 {
		return nil, fmt.Errorf("Can not parse the line: %s, for data format: influx ", line)
	}

	return metrics[0], nil
}

func (p *JimParser) SetDefaultTags(tags map[string]string) {
	p.DefaultTags = tags
}

type JSONFlattener struct {
	Fields map[string]interface{}
}

// FlattenJSON flattens nested maps/interfaces into a fields map
func (f *JSONFlattener) FlattenJSON(
	fieldname string,
	v interface{},
) error {
	if f.Fields == nil {
		f.Fields = make(map[string]interface{})
	}
	fieldname = strings.Trim(fieldname, "_")
	switch t := v.(type) {
	case map[string]interface{}:
		for k, v := range t {
			err := f.FlattenJSON(fieldname+"_"+k+"_", v)
			if err != nil {
				return err
			}
		}
	case []interface{}:
		for i, v := range t {
			k := strconv.Itoa(i)
			err := f.FlattenJSON(fieldname+"_"+k+"_", v)
			if err != nil {
				return nil
			}
		}
	case float64:
		f.Fields[fieldname] = t
	case bool, string, nil:
		// ignored types
		return nil
	default:
		return fmt.Errorf("JSON Flattener: got unexpected type %T with value %v (%s)",
			t, t, fieldname)
	}
	return nil
}
