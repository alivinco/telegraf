package mqtt_iotm_consumer

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	iotm "github.com/alivinco/iotmsglibgo"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
	//"strconv"
	"strconv"
	"errors"
)

var MsgTypeToStringMap = map[iotm.IotMsgType]string{
	iotm.MsgTypeCmd: "cmd",
	iotm.MsgTypeEvt: "evt",
	iotm.MsgTypeGet: "get",
}

type MqttIotMConsumer struct {
	Servers            []string
	Topics             []string
	Username           string
	Password           string
	QoS                int `toml:"qos"`

	parser             parsers.Parser

	// Legacy metric buffer support
	MetricBuffer       int

	PersistentSession  bool
	ClientID           string `toml:"client_id"`

	// Path to CA file
	SSLCA              string `toml:"ssl_ca"`
	// Path to host cert file
	SSLCert            string `toml:"ssl_cert"`
	// Path to cert key file
	SSLKey             string `toml:"ssl_key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool

	DefualtMetricName  string `toml:"default_metric_name"`
	ErrorMetricName    string `toml:"error_metric_name"`
	AlarmMetricName    string `toml:"alarm_metric_name"`
	PingReportMetricName  string `toml:"ping_report_metric_name"`
	//

	sync.Mutex
	client             mqtt.Client
	// channel of all incoming raw mqtt messages
	in                 chan mqtt.Message
	done               chan struct{}

	// keep the accumulator internally:
	acc                telegraf.Accumulator

	started            bool
}

var sampleConfig = `
  servers = ["localhost:1883"]
  ## MQTT QoS, must be 0, 1, or 2
  qos = 0

  ## Topics to subscribe to
  topics = [
    "telegraf/host01/cpu",
    "telegraf/+/mem",
    "sensors/#",
  ]

  # if true, messages that can't be delivered while the subscriber is offline
  # will be delivered when it comes back (such as on service restart).
  # NOTE: if true, client_id MUST be set
  persistent_session = false
  # If empty, a random client ID will be generated.
  client_id = ""

  ## username and password to connect MQTT server.
  # username = "telegraf"
  # password = "metricsmetricsmetricsmetrics"

  ## Optional SSL Config
  # ssl_ca = "/etc/telegraf/ca.pem"
  # ssl_cert = "/etc/telegraf/cert.pem"
  # ssl_key = "/etc/telegraf/key.pem"
  ## Use SSL but skip chain & host verification
  # insecure_skip_verify = false

  ## Data format to consume.
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`

func (m *MqttIotMConsumer) SampleConfig() string {
	return sampleConfig
}

func (m *MqttIotMConsumer) Description() string {
	return "Read metrics from MQTT topic(s)"
}

func (m *MqttIotMConsumer) SetParser(parser parsers.Parser) {
	m.parser = parser
}

func (m *MqttIotMConsumer) Start(acc telegraf.Accumulator) error {
	m.Lock()
	defer m.Unlock()
	m.started = false

	if m.PersistentSession && m.ClientID == "" {
		return fmt.Errorf("ERROR MQTT Consumer: When using persistent_session" +
			" = true, you MUST also set client_id")
	}

	m.acc = acc
	if m.QoS > 2 || m.QoS < 0 {
		return fmt.Errorf("MQTT Consumer, invalid QoS value: %d", m.QoS)
	}

	opts, err := m.createOpts()
	if err != nil {
		return err
	}

	m.client = mqtt.NewClient(opts)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	m.in = make(chan mqtt.Message, 1000)
	m.done = make(chan struct{})

	go m.receiver()

	return nil
}
func (m *MqttIotMConsumer) onConnect(c mqtt.Client) {
	log.Printf("MQTT Client Connected")
	if !m.PersistentSession || !m.started {
		topics := make(map[string]byte)
		for _, topic := range m.Topics {
			topics[topic] = byte(m.QoS)
		}
		subscribeToken := c.SubscribeMultiple(topics, m.recvMessage)
		subscribeToken.Wait()
		if subscribeToken.Error() != nil {
			log.Printf("MQTT SUBSCRIBE ERROR\ntopics: %s\nerror: %s",
				strings.Join(m.Topics[:], ","), subscribeToken.Error())
		}
		m.started = true
	}
	return
}

func (m *MqttIotMConsumer) onConnectionLost(c mqtt.Client, err error) {
	log.Printf("MQTT Connection lost\nerror: %s\nMQTT Client will try to reconnect", err.Error())
	return
}

// receiver() reads all incoming messages from the consumer, and parses them into
// influxdb metric points.

func (p *MqttIotMConsumer) Parse(buf []byte, topic string) (metr []telegraf.Metric, err error) {
	defer func(){
		if r := recover(); r != nil {
		    fmt.Println("Error while parsing message", r)
		    err = errors.New(fmt.Sprintf("%v",r))
		}
	}()
	config := map[string]string{"override_payload_type": "jim0"}
	iotMsg, err := iotm.ConvertBytesToIotMsg("", buf, config)
	if err != nil {
		return nil , err
	}
	fmt.Println(iotMsg.String())
	metrics := make([]telegraf.Metric, 0)
	tags := make(map[string]string)
	f := make(map[string]interface{})
	tags["type"] = MsgTypeToStringMap[iotMsg.Type]
	tags["cls"] = iotMsg.Class
	tags["subcls"] = iotMsg.SubClass
	tags["spid"] = iotMsg.Spid
	if err != nil {
		fmt.Println(err)
	}
	var metricName string
	switch iotMsg.Class {
	case "error":
		tags["code"] ,_ = iotMsg.GetDefaultStr()
		props := *iotMsg.GetProperties()
		node_id, ok := props["node_id"]
		if ok {
			tags["node_id"],_ = ToStr(node_id)
		}
		f["msg"],_ = ToStr(props["error"])
		if f["msg"] == nil {
			f["msg"] = "empty"
		}
		metricName = p.ErrorMetricName
	case "alarm":
		if iotMsg.SubClass == "monitor" {
			tags["status"] ,err = iotMsg.GetDefaultStr()
			props := *iotMsg.GetProperties()
			node_id, ok := props["device"].(map[string]interface{})["value"]
			if ok {
				tags["node_id"],_ = node_id.(string)
			}
			f["a"] = 1
			metricName = p.AlarmMetricName
			break
		}
		fallthrough
	case "net":
		if iotMsg.SubClass == "ping_report"{
			tags["node_id"] ,err = iotMsg.GetDefaultStr()
			props := *iotMsg.GetProperties()
			f["tx_count"],_ = props["tx_count"].(float64)
			f["rx_count"],_ = props["rx_count"].(float64)
			metricName = p.PingReportMetricName
			break
		}
		fallthrough
	default:
		var def interface{}
		def = iotMsg.GetDefault()
		switch def := def.(type) {
		default:
			f["def_str"] = "none"
		case bool:
			f["def_bool"] = def
		case int, int32, int64, float64:
			f["def_num"] = def
		case string:
			f["def_str"] = def
		}
		//f["props"] = fmt.Sprintf("%v",iotMsg.GetProperties())
		metricName = p.DefualtMetricName
	}
	metric, err := telegraf.NewMetric(metricName, tags, f, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return append(metrics, metric), nil
}

func (m *MqttIotMConsumer) receiver() {
	for {
		select {
		case <-m.done:
			return
		case msg := <-m.in:
			topic := msg.Topic()
			var metrics []telegraf.Metric
			var err error
			metrics, err = m.Parse(msg.Payload(), msg.Topic())
			if err != nil {
				log.Printf("MQTT PARSE ERROR\nmessage: %s\nerror: %s",
					string(msg.Payload()), err.Error())
			}

			for _, metric := range metrics {
				tags := metric.Tags()
				tags["topic"] = topic
				m.acc.AddFields(metric.Name(), metric.Fields(), tags, metric.Time())
			}
		}
	}
}

func (m *MqttIotMConsumer) recvMessage(_ mqtt.Client, msg mqtt.Message) {
	m.in <- msg
}

func (m *MqttIotMConsumer) Stop() {
	m.Lock()
	defer m.Unlock()
	close(m.done)
	m.client.Disconnect(200)
	m.started = false
}

func (m *MqttIotMConsumer) Gather(acc telegraf.Accumulator) error {
	return nil
}

func (m *MqttIotMConsumer) createOpts() (*mqtt.ClientOptions, error) {
	opts := mqtt.NewClientOptions()

	if m.ClientID == "" {
		opts.SetClientID("Telegraf-Consumer-" + internal.RandomString(5))
	} else {
		opts.SetClientID(m.ClientID)
	}

	tlsCfg, err := internal.GetTLSConfig(
		m.SSLCert, m.SSLKey, m.SSLCA, m.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	scheme := "tcp"
	if tlsCfg != nil {
		scheme = "ssl"
		opts.SetTLSConfig(tlsCfg)
	}

	user := m.Username
	if user != "" {
		opts.SetUsername(user)
	}
	password := m.Password
	if password != "" {
		opts.SetPassword(password)
	}

	if len(m.Servers) == 0 {
		return opts, fmt.Errorf("could not get host infomations")
	}
	for _, host := range m.Servers {
		server := fmt.Sprintf("%s://%s", scheme, host)

		opts.AddBroker(server)
	}
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(time.Second * 60)
	opts.SetCleanSession(!m.PersistentSession)
	opts.SetOnConnectHandler(m.onConnect)
	opts.SetConnectionLostHandler(m.onConnectionLost)
	return opts, nil
}

func init() {
	inputs.Add("mqtt_iotm_consumer", func() telegraf.Input {
		return &MqttIotMConsumer{}
	})
}

func ToStr(value interface{}) (string,error) {
	switch v := value.(type) {
	case float64 , float32:
		return strconv.FormatFloat(v.(float64), 'f', -1, 64), nil
	case string:
		return v , nil
	case int:
		return strconv.Itoa(v), nil
	case bool:
		return strconv.FormatBool(v) , nil
	default:
		return "", errors.New("Variable can't be converted into string")
	}
}