package main

// TODO: replace print statements with logging

import (
	"flag"
	"fmt"
	"encoding/json"
	"encoding/base64"
	"os"
	"time"
)

import (
	"github.com/garyburd/redigo/redis"
)

var HORIZON uint
var INTERVAL uint
var DEBUG uint
var PATH string
var HOST string
var PORT uint

func init() {
	var help bool
	flag.UintVar(&HORIZON, "horizon", 60*5, "how much time (in seconds) to include in the output file")
	flag.UintVar(&INTERVAL, "interval", 60, "how often (in seconds) to output data")
	flag.UintVar(&DEBUG, "debug", 0, "amount of debug info to print")
	flag.StringVar(&PATH, "output", "/tmp/celerymunin.out", "path to output statistics")
	flag.StringVar(&HOST, "host", "localhost", "redis host to connect to")
	flag.UintVar(&PORT, "port", 6379, "redis port to connect to")
	flag.BoolVar(&help, "help", false, "prints help info")
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
	if err != nil {
		fmt.Println(err)
	}

	body := make(map[string]interface {})
	if err := json.Unmarshal(bodyJs, &body); err != nil {
		return fmt.Errorf("Error unmarshalling json body: %v\n%v\n", err, string(bodyJs))
	}

	msgType, ok := body["type"].(string)
	// not the right type of message, quietly exit
	if !ok {
		if DEBUG >= 2 { fmt.Printf("Invalid body, skipping. %v\n", eventMsg) }
		return nil
	}
	fmt.Println(msgType)
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
	if DEBUG >= 2 { fmt.Println(ev) }

	channel <- ev

	return nil
}

// listens on redis for events
// and stuffs them into the event channel
func listener(channel chan event) {
	for {
		conn, err := redis.Dial("tcp", fmt.Sprintf("%v:%v", HOST, PORT))
		if err != nil {
			fmt.Printf("Error connecting to Redis: %v\n", err)
			fmt.Printf("Waiting 1 sec")
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
						fmt.Printf("Error processing event: %v\n", err)
					} else {
						if DEBUG >= 3 { fmt.Println(string(v.Data)) }
					}

				case redis.Subscription:
					fmt.Printf("Subscribed to: %v", v.Channel)

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
