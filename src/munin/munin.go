package munin

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

import (
	yaml "launchpad.net/goyaml"
)

type GraphType string

const (
	GAUGE = GraphType("GAUGE")
	COUNTER = GraphType("COUNTER")
	DERIVE = GraphType("DERIVE")
)

type GraphName string

const (
	task_success = GraphName("task_success")
	task_started = GraphName("task_started")
	task_failed = GraphName("task_failed")
	task_received = GraphName("task_received")
	task_success_time = GraphName("task_success_time")
	task_failed_time = GraphName("task_failed_time")
	task_start_time = GraphName("task_start_time")
	task_expired = GraphName("task_expired")
	queue_length = GraphName("queue_length")
)

func undot(s string) string {
	return strings.Replace(s, ".", "_", -1)
}

// prepends the parent, and undots the item
func parent(parent_name GraphName, item string) string {
	return fmt.Sprintf("%v_%v", parent_name, undot(item))
}

func Map(src []string, f func(string)string) []string {
	dst := make([]string, 0, len(src))
	for _, s := range src {
		dst = append(dst, f(s))
	}
	return dst
}

// determines which graph config will be shown
var MODE GraphName

func multigraph(entries []string, id GraphName, name string, graph_type GraphType, vlabel string, category string, info string) {
	pp := func(s string) string { return parent(id, s) }

//	fmt.Printf("multigraph %v\n", strings.ToLower(strings.Replace(name, " ", "_", -1)))
	fmt.Printf("graph_title %v\n", name)
	fmt.Printf("graph_order %v\n", strings.Join(Map(entries, pp), " "))
	fmt.Println("graph_args --base 1000 --lower-limit 0")
	fmt.Printf("graph_vlabel %v\n", vlabel)
	fmt.Printf("graph_category %v\n", category)
	fmt.Printf("graph_info %v\n", info)
	fmt.Println("")

	for _, entry := range entries {
		fmt.Printf("%v.label %v\n", pp(entry), entry)
		fmt.Printf("%v.type %v\n", pp(entry), graph_type)
		fmt.Printf("%v.min 0\n", pp(entry))
	}
	fmt.Println("")
}

// prints config info for the tasks
func configure(tasks []string, queues []string) {
	//task graphs
	if len(tasks) > 0 {
		if MODE == task_success {
			multigraph(
				tasks,
				task_success,
				"Successful Tasks",
				GAUGE,
				"Successful tasks per 5 min",
				"tasks",
				"This graph shows the number of successful tasks per 5min",
			)
		}
		if MODE == task_started {
			multigraph(
				tasks,
				task_started,
				"Started Tasks",
				GAUGE,
				"Started tasks per 5 min",
				"tasks",
				"This graph shows the number of tasks started per 5min",
			)
		}
		if MODE == task_failed {
			multigraph(
				tasks,
				task_failed,
				"Failed Tasks",
				GAUGE,
				"Failed tasks per 5 min",
				"tasks",
				"This graph shows the number of failed tasks per 5min",
			)
		}
		if MODE == task_received {
			multigraph(
				tasks,
				task_received,
				"Received Tasks",
				GAUGE,
				"Tasks receivedper 5 min",
				"tasks",
				"This graph shows the number of tasks received per 5min",
			)
		}
		if MODE == task_success_time {
			multigraph(
				tasks,
				task_success_time,
				"Success Time",
				GAUGE,
				"Avg time to task success",
				"tasks",
				"This graph shows the average time to completion for successful tasks",
			)
		}
		if MODE == task_failed_time {
			multigraph(
				tasks,
				task_failed_time,
				"Failure Time",
				GAUGE,
				"Avg time to task failure",
				"tasks",
				"This graph shows the average time to fail for tasks",
			)
		}
		if MODE == task_start_time {
			multigraph(
				tasks,
				task_start_time,
				"Start Time",
				GAUGE,
				"Avg time to start task",
				"tasks",
				"This graph shows the average time it takes a task to start after it's received",
			)
		}
		if MODE == task_expired {
			multigraph(
				tasks,
				task_expired,
				"Abandoned tasks expired",
				GAUGE,
				"Abandoned tasks expired",
				"tasks",
				"This graph shows tasks that never terminated for some reason, and were culled from the dataset",
			)
		}
	}
	if len(queues) > 1 {
		if MODE == queue_length {
			multigraph(
				queues,
				queue_length,
				"Queue Size",
				GAUGE,
				"Queue Size",
				"queues",
				"This graph shows queue sizes over time",
			)
		}
	}
}

// prints metrics for the tasks
func output(tasks []string, queues []string) {

	get_int := func(i interface {}) int {
		v, _ := i.(int)
		return v
	}
	get_float := func(i interface {}) float64 {
		v, _ := i.(float64)
		return v
	}

	data_path := os.Getenv("data_path")
	if data_path == "" { data_path = "/tmp/celerymunin.out" }

	data := make(map[string] interface {})
	if b, err := ioutil.ReadFile(data_path); err == nil {
		if err = yaml.Unmarshal(b, data); err != nil {
			fmt.Println("#", err)
		}
	} else {
		fmt.Println("#", err)
	}

	task_data, _ := data["tasks"].(map[interface {}]interface {})
	queue_data, _ := data["queues"].(map[interface {}]interface {})

	for _, task := range tasks {
		td, _ := task_data[task].(map[interface {}] interface{})
		fmt.Printf("%v.value %v\n", parent(task_received, task), get_int(td["num_received"]))
		fmt.Printf("%v.value %v\n", parent(task_started, task), get_int(td["num_started"]))
		fmt.Printf("%v.value %v\n", parent(task_failed, task), get_int(td["num_failed"]))
		fmt.Printf("%v.value %v\n", parent(task_success, task), get_int(td["num_success"]))
		fmt.Printf("%v.value %v\n", parent(task_expired, task), get_int(td["expired"]))
		fmt.Printf("%v.value %f\n", parent(task_start_time, task), get_float(td["start_time"]))
		fmt.Printf("%v.value %f\n", parent(task_success_time, task), get_float(td["success_time"]))
		fmt.Printf("%v.value %f\n", parent(task_failed_time, task), get_float(td["failure_time"]))
	}

	for _, queue := range queues {
		fmt.Printf("%v.value %v\n", parent(queue_length, queue), get_int(queue_data[queue]))
	}
}

// separates by comma, trims, sorts, and returns
func comma_separate(s string) []string {
	ss := strings.Split(s, ",")
	entries := make([]string, 0, len(ss))
	for _, entry := range ss {
		entry = strings.Trim(entry, " ")
		if len(entry) > 0 {
			entries = append(entries, entry)
		}
	}
	sort.Strings(entries)
	return entries
}

func RunMuninPlugin() {
	tasks := comma_separate(os.Getenv("tasks"))
	queues := comma_separate(os.Getenv("queues"))
	if len(tasks) == 0  && len(queues) == 0 {
		fmt.Println("Need at least one task or queue")
		os.Exit(1)
	}

	call := os.Args[0]
	split_call := strings.Split(call, "/")
	file := split_call[len(split_call) - 1]
	file_split := strings.Split(file, "_")
	MODE = GraphName(strings.Join(file_split[1:], "_"))

	fmt.Println("# Celery Munin")
	fmt.Println("# tasks", tasks)
	fmt.Println("# queues", queues)
	fmt.Println("# mode", MODE)


	if len(os.Args) > 1 && os.Args[1] == "config" {
		configure(tasks, queues)
	} else {
		output(tasks, queues)
	}
}
