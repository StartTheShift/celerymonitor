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
	GetID() TaskId
	GetTimestamp() time.Time
	GetReceived() time.Time
}

type baseEvent struct {
	taskId TaskId
	eventTime time.Time
	receivedTime time.Time
}
func (e *baseEvent) GetID() TaskId { return e.taskId }
func (e *baseEvent) GetTimestamp() time.Time { return e.eventTime }
func (e *baseEvent) GetReceived() time.Time { return e.receivedTime }
func (e *baseEvent) String() string { return fmt.Sprintf("id: %v, time: %v", e.taskId, e.eventTime) }

type Received struct {
	baseEvent
	taskName string
}

func (e *Received) GetName() string { return e.taskName }
func (e *Received) String() string { return fmt.Sprintf("Task Received. name: %v, %v", e.taskName, e.baseEvent.String()) }

type Started struct {
	baseEvent
}
func (e *Started) String() string { return fmt.Sprintf("Task Started. %v", e.baseEvent.String()) }

type Success struct {
	baseEvent
}
func (e *Success) String() string { return fmt.Sprintf("Task Success. %v", e.baseEvent.String()) }

type Failure struct {
	baseEvent
}
func (e *Failure) String() string { return fmt.Sprintf("Task Failed. %v", e.baseEvent.String()) }

// converts a fractional second unix timestamp to time.Time
func floatToTime(ts float64) time.Time {
	seconds := int64(ts)
	fraction := ts - float64(seconds)
	nanoseconds := int64(float64(time.Second) * fraction)
	return time.Unix(seconds, nanoseconds)
}

func _sendEvent(channel chan<- event, data []byte, timeReceived time.Time) error {
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
		logger.Debug("Ignored event type: %v", msgType)
		return nil
	}

	taskId, ok := body["uuid"].(TaskId)
	if !ok { return fmt.Errorf("event did not include type uuid, or it was the wrong type: %v", body["uuid"]) }
	seconds, ok := body["timestamp"].(float64)
	if !ok { return fmt.Errorf("event did not include type timestamp, or it was the wrong type: %v", body["timestamp"]) }
	timestamp := floatToTime(seconds)

	var ev event
	switch msgType {
	case "task-received":
		name, ok := body["name"].(string)
		if !ok { return fmt.Errorf("event name not found, or it was the wrong type: %v", body["type"]) }
		ev = &Received{taskName:name, baseEvent:baseEvent{taskId:taskId, eventTime:timestamp, receivedTime:timeReceived}}
	case "task-started":
		ev = &Started{baseEvent{taskId:taskId, eventTime:timestamp, receivedTime:timeReceived}}
	case "task-succeeded":
		ev = &Success{baseEvent{taskId:taskId, eventTime:timestamp, receivedTime:timeReceived}}
	case "task-failed":
		ev = &Failure{baseEvent{taskId:taskId, eventTime:timestamp, receivedTime:timeReceived}}
	default:
		return fmt.Errorf("Unknown event type: %v", msgType)
	}
	logger.Debug("%v", ev)

	channel <- ev

	return nil
}

// listens on redis for events
// and stuffs them into the event channel
func listener(channel chan<- event) {
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
					t := time.Now()
					if err := _sendEvent(channel, v.Data, t); err != nil {
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
	logger.Panic("listener loop stopped")
}

// task tracking
type TaskId string

type TaskTracker struct {
	name string
	received map[TaskId] *Received
	started map[TaskId] *Started
	success map[TaskId] *Success
	failure map[TaskId] *Failure
}

func NewTaskTracker(name string) *TaskTracker {
	tt := &TaskTracker{
		name:name,
		received:make(map[TaskId]*Received),
		started:make(map[TaskId]*Started),
		success:make(map[TaskId]*Success),
		failure:make(map[TaskId]*Failure),
	}
	return tt
}

func (t *TaskTracker) ReceiveEvent(newEv event) error {
	switch ev := (newEv).(type) {
	case *Received:
		t.received[ev.GetID()] = ev
	case *Started:
		t.started[ev.GetID()] = ev
	case *Success:
		t.success[ev.GetID()] = ev
	case *Failure:
		t.failure[ev.GetID()] = ev
	default:
		return fmt.Errorf("Unhandled event type: %T", ev)
	}
	return nil
}

// removes old tasks, and returns a list
// of purged task ids
func (t *TaskTracker) Clean() []TaskId {
	return []TaskId{}
}

var trackers map[string] *TaskTracker
var idTrackerMap map[TaskId] *TaskTracker

func _aggregate(start time.Time) {

}

// records the event data
func recorder(eventChan <-chan event, aggregateSignal <-chan time.Time) {
	for {
		select {
		case ev := <- eventChan:
			var tracker *TaskTracker

			if receiveMsg, ok := ev.(*Received); ok {
				name := receiveMsg.GetName()
				tracker = trackers[name]
				if trackers == nil {
					tracker := NewTaskTracker(name)
					trackers[name] = tracker
				}
				idTrackerMap[receiveMsg.GetID()] = tracker
			}
			tracker= idTrackerMap[ev.GetID()]
			if trackers == nil {
				logger.Info("No tracker found for task %v", ev.GetID())
				continue
			}
			tracker.ReceiveEvent(ev)

		case aggStart := <- aggregateSignal:
			logger.Debug("Aggregate signal received at %v", aggStart)
			//
		}

	}
	logger.Panic("recorder loop stopped")
}

// aggregates and writes event data
func aggregate(start time.Time) {
	// aggregate data
	// evict old tasks
}

func main() {
	eventChan := make(chan event, 200)
	aggregateSignal := make(chan time.Time)
	go listener(eventChan)
	go recorder(eventChan, aggregateSignal)

	// start periodically sending aggregation signals to the recorder
	aggregateEvent := time.After(time.Duration(INTERVAL) * time.Second)
	for {
		select {
		case start := <- aggregateEvent:
			// do aggregation
			aggregateEvent = time.After(time.Duration(INTERVAL) * time.Second)
			aggregateSignal <- start
		}
	}
}
