package jim

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	iotm "github.com/alivinco/iotmsglibgo"
	"github.com/influxdata/telegraf"
)

var MsgTypeToStringMap = map[iotm.IotMsgType]string{
		iotm.MsgTypeCmd : "cmd",
		iotm.MsgTypeEvt : "evt",
		iotm.MsgTypeGet : "get",
	}

type JimParser struct {
	MetricName  string
	TagKeys     []string
	DefaultTags map[string]string
}
func (p *JimParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	config := map[string]string{"override_payload_type":"jim0"}
	iotMsg , err := iotm.ConvertBytesToIotMsg("",buf,config)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(iotMsg.String())
	metrics := make([]telegraf.Metric, 0)
	tags := make(map[string]string)
	tags["type"] = MsgTypeToStringMap[iotMsg.Type]
	tags["cls"] = iotMsg.Class
	tags["subcls"] = iotMsg.SubClass
	f := make(map[string]interface{})
	var def interface{}
	def = iotMsg.GetDefault()
	switch def := def.(type){
	default:
		f["def_str"] = "none"
	case bool :
		f["def_bool"] = def
	case int :
		f["def_num"] = def
	case string:
		f["def_str"] = def
	}
	metric, err := telegraf.NewMetric(p.MetricName, tags, f, time.Now().UTC())

	if err != nil {
		return nil, err
	}
	return append(metrics, metric), nil
}

func (p *JimParser) ParseLine(line string) (telegraf.Metric, error) {
	metrics, err := p.Parse([]byte(line + "\n"))

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

