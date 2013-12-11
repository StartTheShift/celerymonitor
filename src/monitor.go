package main

// TODO: replace print statements with logging

import (
	"flag"
	"fmt"
	"encoding/json"
	"encoding/base64"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

import (
	// aliased import
	logging "github.com/op/go-logging"
	"github.com/garyburd/redigo/redis"
)

var HORIZON uint
var INTERVAL uint
var DEBUG uint
var PATH string
var HOST string
var PORT uint
var LOGPATH string
var LOGLEVEL string
var logger *logging.Logger

func init() {
	var help bool
	// aggregations
	flag.UintVar(&HORIZON, "horizon", 60*5, "how much time (in seconds) to include in the output file")
	flag.UintVar(&INTERVAL, "interval", 60, "how often (in seconds) to output data")
	flag.StringVar(&PATH, "output", "/tmp/celerymunin.out", "path to output statistics")

	flag.UintVar(&DEBUG, "debug", 0, "amount of debug info to print")

	// redis
	flag.StringVar(&HOST, "host", "localhost", "redis host to connect to")
	flag.UintVar(&PORT, "port", 6379, "redis port to connect to")
	flag.BoolVar(&help, "help", false, "prints help info")

	// logging
	flag.StringVar(&LOGPATH, "log-path", "stdout", "logging path, stderr and stdout work too")
	flag.StringVar(&LOGLEVEL, "log-level", "WARNING", "logging level")

	flag.Parse()
	if help {
		fmt.Println("monitors celery events and periodically saves to disk for use in monitoring applications")
		flag.Usage()
		os.Exit(0)
	}

	// validate command params
	if HORIZON == uint(0) {
		fmt.Println("horizon must be greater than 0")
		os.Exit(1)
	}
	if INTERVAL == uint(0) {
		fmt.Println("interval must be greater than 0")
		os.Exit(1)
	}
	if PATH == "" {
		fmt.Println("path cannot be blank")
		os.Exit(1)
	}
	logging.SetBackend()

	// logging
	LOGLEVEL = strings.ToUpper(LOGLEVEL)
	if level, err := logging.LogLevel(LOGLEVEL); err != nil {
		fmt.Printf("Bad log level: %v\n", err)
		os.Exit(1)
	} else {
		logging.SetLevel(level, "monitor")
	}

	if LOGPATH == "" {
		fmt.Println("path cannot be blank")
		os.Exit(1)
	} else {
		var logOut io.Writer
		if strings.ToUpper(LOGPATH) == "STDOUT" {
			logOut = os.Stdout
		} else if strings.ToUpper(LOGPATH) == "STDERR" {
			logOut = os.Stderr
		} else {
			var err error
			logOut, err = os.Open(LOGPATH)
			if err != nil {
				fmt.Println("Error opening log path: ", err)
				os.Exit(1)
			}
		}
		logging.SetBackend(logging.NewLogBackend(logOut, "", log.LstdFlags))
	}
	logger = logging.MustGetLogger("monitor")
}

type event interface {
	GetID() string
	GetTimestamp() float64
}

type baseEvent struct {
	taskId string
	timestamp float64
}
func (e *baseEvent) GetID() string { return e.taskId }
func (e *baseEvent) GetTimestamp() float64 { return e.timestamp }
func (e *baseEvent) String() string { return fmt.Sprintf("id: %v, time: %v", e.taskId, e.timestamp) }

type received struct {
	baseEvent
	taskName string
}

func (e *received) GetName() string { return e.taskName }
func (e *received) String() string { return fmt.Sprintf("Task Received. name: %v, %v", e.taskName, e.baseEvent.String()) }

type started struct {
	baseEvent
}
func (e *started) String() string { return fmt.Sprintf("Task Started. %v", e.baseEvent.String()) }

type success struct {
	baseEvent
}
func (e *success) String() string { return fmt.Sprintf("Task Success. %v", e.baseEvent.String()) }

type failure struct {
	baseEvent
}
func (e *failure) String() string { return fmt.Sprintf("Task Failed. %v", e.baseEvent.String()) }

func _sendEvent(channel chan<- event, data []byte) error {
	eventMsg := make(map[string]interface {})
	if err := json.Unmarshal(data, &eventMsg); err != nil {
		return fmt.Errorf("Error unmarshalling json: %v\n%v\n", err, string(data))
	}

	body64, ok := eventMsg["body"].(string)
	if !ok {
		return fmt.Errorf("event did not include body field, or it was the wrong type: %v", eventMsg["body"])
	}

	bodyJs, err := base64.URLEncoding.DecodeString(body64)
	if err != nil { return err }

	body := make(map[string]interface {})
	if err := json.Unmarshal(bodyJs, &body); err != nil {
		return fmt.Errorf("Error unmarshalling json body: %v\n%v\n", err, string(bodyJs))
	}

	msgType, ok := body["type"].(string)
	// not the right type of message, quietly exit
	if !ok {
		logger.Debug("Invalid body, skipping. %v", eventMsg)
		return nil
	}

	// check that this is a supported event
	supported := false
	for _, t := range []string{"task-received", "task-started", "task-succeeded", "task-failed"} {
		supported = supported || (msgType == t)
	}
	if !supported {
		logger.Debug("Unsupported event type: %v", msgType)
		return nil
	}

	taskId, ok := body["uuid"].(string)
	if !ok { return fmt.Errorf("event did not include type uuid, or it was the wrong type: %v", body["uuid"]) }
	timestamp, ok := body["timestamp"].(float64)
	if !ok { return fmt.Errorf("event did not include type timestamp, or it was the wrong type: %v", body["timestamp"]) }

	var ev event
	switch msgType {
	case "task-received":
		name, ok := body["name"].(string)
		if !ok { return fmt.Errorf("event name not found, or it was the wrong type: %v", body["type"]) }
		ev = &received{taskName:name, baseEvent:baseEvent{taskId:taskId, timestamp:timestamp}}
	case "task-started":
		ev = &started{baseEvent{taskId:taskId, timestamp:timestamp}}
	case "task-succeeded":
		ev = &success{baseEvent{taskId:taskId, timestamp:timestamp}}
	case "task-failed":
		ev = &failure{baseEvent{taskId:taskId, timestamp:timestamp}}
	default:
		return fmt.Errorf("Unknown event type: %v", msgType)
	}
	logger.Debug("%+v", ev)

	channel <- ev

	return nil
}

// listens on redis for events
// and stuffs them into the event channel
func listener(channel chan event) {
	for {
		conn, err := redis.Dial("tcp", fmt.Sprintf("%v:%v", HOST, PORT))
		if err != nil {
			logger.Error("Error connecting to Redis: %v\nWaiting 1 sec", err)
			time.Sleep(1 * time.Second)
			continue
		}
		psc := redis.PubSubConn{conn}
		psc.Subscribe("celeryev")
		eventLoop:
			for {
				switch v := psc.Receive().(type) {
				case redis.Message:
					if err := _sendEvent(channel, v.Data); err != nil {
						logger.Error("Error processing event: %v", err)
					} else {
						if DEBUG >= 3 { fmt.Println(string(v.Data)) }
					}

				case redis.Subscription:
					logger.Info("Subscribed to: %v", v.Channel)

				case error:
					// kill connection and start over
					conn.Close()
					break eventLoop

				default:
					fmt.Println("Unknown type", v)
				}
			}
	}
}

func main() {
	eventChan := make(chan event, 200)
	listener(eventChan)
	dd := "{\"body\": \"eyJ1dWlkIjogIjZlYzc5ZjIxLTY4ZjEtNDFiNy1hNWUzLTNiMWUzZWU4OWFmZSIsICJjbG9jayI6IDcyLCAidGltZXN0YW1wIjogMTM4NjcwNDM1Ny45NTI1NTQsICJob3N0bmFtZSI6ICJCbGFrZXMtTWFjQm9vay1Qcm8ubG9jYWwiLCAicGlkIjogMjI5NDIsICJ0eXBlIjogInRhc2stc3RhcnRlZCJ9\", \"headers\": {}, \"content-type\": \"application/json\", \"properties\": {\"body_encoding\": \"base64\", \"delivery_info\": {\"priority\": 0, \"routing_key\": \"task.started\", \"exchange\": \"celeryev\"}, \"delivery_mode\": 2, \"delivery_tag\": \"49ac8bc1-53ad-4e39-ba47-075de6e2615c\"}, \"content-encoding\": \"utf-8\"}"
	//	var ev1 event1
	ev1 := make(map[string]interface {})

	if err := json.Unmarshal([]byte(dd), &ev1); err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("\n%+v\n", ev1)
	}

	//	ev2 := make(map[string]interface {})
	src := ev1["body"].(string)
	fmt.Println(src)
	data, err := base64.URLEncoding.DecodeString(src)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(data)
	fmt.Println(string(data))
	ev2 := make(map[string]interface {})
	if err := json.Unmarshal(data, &ev2); err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("\n%+v\n", ev2)
		ev1["body"] = ev2
		fmt.Printf("\n%+v\n", ev1)
	}

	fmt.Printf("Hello world!")
	fmt.Printf("Hello world!")
	fmt.Printf("Hello world!")
}
