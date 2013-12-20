package monitor

// TODO: replace print statements with logging

import (
	"flag"
	"fmt"
	"encoding/json"
	"encoding/base64"
	"expvar"
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
	yaml "launchpad.net/goyaml"
)

var HORIZON uint
var INTERVAL uint
var PATH string
var HOST string
var PORT uint
var TTL uint
var LOGPATH string
var LOGLEVEL string
var QUEUES []string
var logger *logging.Logger

func init() {
	var help bool
	// aggregations
	flag.UintVar(&HORIZON, "horizon", 60*5, "how much time (in seconds) to include in the output file")
	flag.UintVar(&INTERVAL, "interval", 60, "how often (in seconds) to output data")
	flag.StringVar(&PATH, "output", "/tmp/celerymunin.out", "path to output statistics")

	flag.UintVar(&TTL, "ttl", 12*60*60, "how long an unterminated task will be retained for (prevents memory leaks by culling orphaned tasks)")

	// redis
	flag.StringVar(&HOST, "host", "localhost", "redis host to connect to")
	flag.UintVar(&PORT, "port", 6379, "redis port to connect to")
	flag.BoolVar(&help, "help", false, "prints help info")

	// logging
	flag.StringVar(&LOGPATH, "log-path", "stdout", "logging path, stderr and stdout work too")
	flag.StringVar(&LOGLEVEL, "log-level", "WARNING", "logging level")

	var queues string
	flag.StringVar(&queues, "queues", "", "comma separated list of queues to monitor, if any")

	flag.Parse()
	if help {
		fmt.Println("monitors celery events and periodically saves to disk for use in monitoring applications")
		flag.Usage()
		os.Exit(0)
	}

	// logging
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

	LOGLEVEL = strings.ToUpper(LOGLEVEL)
	if level, err := logging.LogLevel(LOGLEVEL); err != nil {
		logger.Fatalf("Bad log level: %v\n", err)
		os.Exit(1)
	} else {
		logging.SetLevel(level, "monitor")
	}

	QUEUES = strings.Split(queues, ",")
	for idx, queue := range QUEUES {
		QUEUES[idx] = strings.Trim(queue, " ")
	}
	logger.Info("Monitoring queues: %+v", QUEUES)


	// validate command params
	if HORIZON == uint(0) {
		logger.Fatal("horizon must be greater than 0")
	}
	logger.Info("Horizon set at %v seconds", HORIZON)


	if INTERVAL == uint(0) {
		logger.Fatal("interval must be greater than 0")
	}
	logger.Info("Interval set at %v seconds", INTERVAL)

	if PATH == "" {
		logger.Fatal("path cannot be blank")
	} else if strings.ToUpper(PATH) == "STDOUT" || strings.ToUpper(PATH) == "STDERR" {
		// do nothing
	} else {
		verifyPath := func(p string) {
			nfo, err := os.Stat(p)
			if err != nil {
				if os.IsNotExist(err) {
					fp, err := os.Create(p)
					if err != nil {
						logger.Fatalf("Error opening output path [%v] for writing: %v\n", p, err)
					}
					fp.Close()
				} else {
					logger.Fatalf("Error getting info on logpath [%v]: %v\n", p, err)
				}
			} else {
				if nfo.IsDir() {
					logger.Fatalf("Output path [%v] is a directory\n", p)
				}
			}
			fp, err := os.OpenFile(p, os.O_RDWR, 0644)
			if err != nil {
				logger.Fatalf("Error opening output path [%v] for writing: %v\n", p, err)
			}
			fp.Close()
		}
		verifyPath(PATH)
		verifyPath(PATH + ".work")
	}
	logger.Info("Output path: %v", PATH)
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

	taskUUID, ok := body["uuid"].(string)
	if !ok { return fmt.Errorf("event did not include type uuid, or it was the wrong type: %v", body["uuid"]) }
	taskId := TaskId(taskUUID)
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
//	logger.Debug("New Event: %v", ev)

	channel <- ev

	return nil
}

// listens on redis for events
// and stuffs them into the event channel
func listener(channel chan<- event) {
	logger.Info("Starting listener loop")
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
						logger.Debug(string(v.Data))
					}

				case redis.Subscription:
					logger.Info("Subscribed to: %v", v.Channel)

				case error:
					// kill connection and start over
					conn.Close()
					break eventLoop

				default:
					logger.Debug("Unknown type", v)
				}
			}
	}
	logger.Panic("listener loop stopped")
}

// task tracking
type TaskId string

type TaskState struct {
	taskId TaskId
	received time.Time
	started time.Time
	terminated time.Time
	successful bool
	lastSeen time.Time
	newInfo bool
}

func (t *TaskState) CanEvict() bool {
	return !t.terminated.IsZero()
}

type TaskStat struct {
	name string
	numReceived uint32
	numStarted uint32
	numSuccess uint32
	numFailed int32
	startLag float64
	successTime float64
	failureTime float64
}

func (ts *TaskStat) Object() map[string] interface {} {
	return map[string] interface {} {
		"name":ts.name,
		"num_received":ts.numReceived,
		"num_started":ts.numStarted,
		"num_success":ts.numSuccess,
		"num_failed":ts.numFailed,
		"start_time":ts.startLag,
		"success_time":ts.successTime,
		"failure_time":ts.failureTime,
	}
}

type TaskTracker struct {
	name string
	states map[TaskId] *TaskState
}

func NewTaskTracker(name string) *TaskTracker {
	tt := &TaskTracker{
		name:name,
		states:make(map[TaskId]*TaskState),
	}
	return tt
}

func (t *TaskTracker) ReceiveEvent(newEv event) error {

	var ts *TaskState
	if _, ok := newEv.(*Received); ok {
		ts = &TaskState{taskId:newEv.GetID()}
		t.states[newEv.GetID()] = ts
	} else {
		ts = t.states[newEv.GetID()]
		if ts == nil {
			return fmt.Errorf("Unrecognized task: %v", newEv.GetID())
		}
	}

	switch ev := (newEv).(type) {
	case *Received:
		ts.received = newEv.GetReceived()
	case *Started:
		ts.started = newEv.GetReceived()
	case *Success:
		ts.terminated = newEv.GetReceived()
		ts.successful = true
	case *Failure:
		ts.terminated = newEv.GetReceived()
		ts.successful = false
	default:
		return fmt.Errorf("Unhandled event type: %T", ev)
	}
	return nil
}

// aggregates all of the info after the horizon
func (t *TaskTracker) Aggregate(horizon time.Time) *TaskStat {
	ts := &TaskStat{name:t.name}
	for _, s := range t.states {
		if s.received.After(horizon) {
			ts.numReceived++
		}
		if s.started.After(horizon) {
			ts.numStarted++
			ts.startLag += float64(s.started.Sub(s.received)) / float64(time.Second)
		}
		if s.terminated.After(horizon) {
			if s.successful {
				ts.numSuccess++
				ts.successTime += float64(s.terminated.Sub(s.started)) / float64(time.Second)
			} else {
				ts.numFailed++
				ts.failureTime += float64(s.terminated.Sub(s.started)) / float64(time.Second)
			}
		}
	}

	// compute averages
	if ts.numStarted > 0 { ts.startLag = ts.startLag / float64(ts.numStarted) }
	if ts.numSuccess > 0 { ts.successTime = ts.successTime / float64(ts.numSuccess) }
	if ts.numFailed > 0 { ts.failureTime = ts.failureTime / float64(ts.numFailed) }

	return ts
}

// removes all states that have terminated
func (t *TaskTracker) CleanupTerminated() []TaskId {
	keys := make([]TaskId, 0, len(t.states))
	for key := range t.states {
		keys = append(keys, key)
	}

	now := time.Now()
	evicted := make([]TaskId, 0, len(t.states))
	for _, key := range keys {
		state, exists := t.states[key]
		if !exists {
			evicted = append(evicted, key)
			continue
		} else if state.CanEvict() {
			delete(t.states, key)
			evicted = append(evicted, key)
		} else if state.received.Before(now.Add(-time.Duration(TTL) * time.Second)) {
			delete(t.states, key)
			evicted = append(evicted, key)
		}
	}
	return evicted
}

var trackers map[string] *TaskTracker
var idTrackerMap map[TaskId] *TaskTracker

func init() {
	trackers = make(map[string] *TaskTracker)
	idTrackerMap = make(map[TaskId] *TaskTracker)
	expvar.Publish("num_trackers", expvar.Func(func () interface {} { return len(trackers) }))
	expvar.Publish("id_tracker_size", expvar.Func(func () interface {} { return len(idTrackerMap) }))
	expvar.Publish("states", expvar.Func(func () interface {} {
			lmap := make(map[string] int)
			for key, tracker := range trackers {
				lmap[key] = len(tracker.states)
			}
			return lmap
		}))
}

func output(stats interface {}) {
	logger.Debug("Outputting stats to: %v", PATH)
	data, err := yaml.Marshal(stats)
	if err != nil {
		logger.Error("Error marshalling stats: %v", err)
	}
	if strings.ToUpper(PATH) == "STDOUT" {
		os.Stdout.Write([]byte("\n\n"))
		os.Stdout.Write(data)
		os.Stdout.Write([]byte("\n\n"))
	} else if strings.ToUpper(PATH) == "STDOUT" {
		os.Stderr.Write([]byte("\n\n"))
		os.Stderr.Write(data)
		os.Stderr.Write([]byte("\n\n"))
	} else {
		fp, err := os.Create(PATH + ".work")
		if err != nil {
			logger.Error("Error opening output path [%v] for writing: %v\n", PATH, err)
		} else {
			if _, err := fp.Write(data); err != nil {
				logger.Error("Error writing data to output file: %v", err)
			}
			fp.Close()
		}
		if err := os.Rename(PATH + ".work", PATH); err != nil {
			logger.Error("Error overwriting previous output file: %v", err)
		}
	}
}

func cleanup() []TaskId {
	to_evict := make([]TaskId, 0, 1000)
	for _, tracker := range trackers {
		evicted := tracker.CleanupTerminated()
		if len(evicted) > 0 {
			to_evict = append(to_evict, evicted...)
		}
	}
	for _, key := range to_evict {
		delete(idTrackerMap, key)
	}
	return to_evict
}

func aggregate(start time.Time) {
	logger.Info("New aggregation started for %v", start)
	horizon := start.Add(-time.Duration(HORIZON) * time.Second)
	logger.Info("Aggregating events between %v & %v", horizon, start)
	if horizon.After(start) { panic("horizon if after start") }
	result := make(map[string] interface {})
	tasks := make(map[string] interface {})
	for name, tracker := range trackers {
		ts := tracker.Aggregate(horizon)
		tasks[name] = ts.Object()
	}
	result["tasks"] = tasks

	// get queues
	queues := make(map[string] interface {})
	_ = queues
	conn, err := redis.Dial("tcp", fmt.Sprintf("%v:%v", HOST, PORT))
	_ = conn
	if err != nil {
		logger.Error("Error connecting to Redis: %v\nWaiting 1 sec", err)
		return
	}
	for _, queue := range QUEUES {
		logger.Debug("Getting queue length for %v", queue)
		conn.Send("llen", queue)
		conn.Flush()
		if response, err := conn.Receive(); err != nil {
			logger.Error("Error getting queue length %v from redis: %v", queue, err)
		} else {
			if size, ok := response.(int64); !ok {
				logger.Error("Unexpected llen response type. Expected int64, got %T", response)
			} else {
				queues[queue] = size
			}
		}
	}
	result["queues"] = queues
	logger.Debug("Aggregation finished for %v", start)
	output(result)
	cleanup()

}


// records the event data
func recorder(eventChan <-chan event, aggregateSignal <-chan time.Time) {
	logger.Info("Starting recorder loop")
	for {
		select {
		case ev := <- eventChan:

			if receiveMsg, ok := ev.(*Received); ok {
				name := receiveMsg.GetName()
				tracker := trackers[name]
				if tracker == nil {
					tracker := NewTaskTracker(name)
					trackers[name] = tracker
					logger.Info("Tracker created for %v", name)
				}
				idTrackerMap[receiveMsg.GetID()] = tracker
			}
			tracker := idTrackerMap[ev.GetID()]
			if tracker == nil {
				logger.Info("No tracker found for task %v", ev.GetID())
				continue
			}
			tracker.ReceiveEvent(ev)

		case aggStart := <- aggregateSignal:
			logger.Debug("Aggregate signal received at %v", aggStart)
			aggregate(aggStart)
			//
		}

	}
	logger.Panic("recorder loop stopped")
}

func RunMonitor() {
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
