package monitor

import (
	"testing"
	"time"
)

func resetTests() {
	trackers = make(map[string] *TaskTracker)
	idTrackerMap = make(map[TaskId] *TaskTracker)
}

// tests that aggregation works properly
func TestAggregation(t *testing.T) {
	defer resetTests()

	t0 := time.Now()
	t1 := t0.Add(30 * time.Second)
	t2 := t0.Add(60 * time.Second)
	t3 := t0.Add(90 * time.Second)
	t4 := t0.Add(120 * time.Second)

	tracker := NewTaskTracker("a")
	s0 := &TaskState{taskId:"0", received:t0}
	s1 := &TaskState{taskId:"1", received:t0, started:t0, terminated:t1}
	s2 := &TaskState{taskId:"2", received:t1, started:t2, terminated:t3, successful:true}
	s3 := &TaskState{taskId:"3", received:t1, started:t2, terminated:t4, successful:true}
	tracker.states["0"] = s0
	tracker.states["1"] = s1
	tracker.states["2"] = s2
	tracker.states["3"] = s3

	var stat *TaskStat

	// test that s0 is not included in received
	stat = tracker.Aggregate(t1.Add(-1 * time.Second))
	if stat.numReceived != 2 {
		t.Errorf("Expected 3 received, got %v", stat.numReceived)
	}
	if stat.failureTime != 30.0 {
		t.Errorf("Expected 30 fail time, got %f", stat.failureTime)
	}
	if stat.successTime != 45.0 {
		t.Errorf("Expected 30 success time, got %f", stat.successTime)
	}
	if stat.startLag != 30.0 {
		t.Errorf("Expected 30 start lag, got %f", stat.startLag)

	}

}

// tests that cleanup works properly
func TestCleanup(t *testing.T) {
	defer resetTests()

}

