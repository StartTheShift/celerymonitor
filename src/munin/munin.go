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
	queue_length = GraphName("queue_length")
)

func undot(s string) string {
	return strings.Replace(s, ".", "_", -1)
}

// prepends the parent, and undots the item
func parent(parent GraphName, item string) string {
	return fmt.Sprintf("%v_%v", parent, undot(item))
}

func Map(src []string, f func(string)string) []string {
	dst := make([]string, 0, len(src))
	for _, s := range src {
		dst = append(dst, f(s))
	}
	return dst
}

func multigraph(entries []string, id GraphName, name string, graph_type GraphType, vlabel string, category string, info string) {
	pp := func(s string) string { return parent(id, s) }

	fmt.Printf("multigraph %v\n", strings.ToLower(strings.Replace(name, " ", "_", -1)))
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
		multigraph(
			tasks,
			task_success,
			"Successful Tasks",
			GAUGE,
			"Successful tasks per 5 min",
			"tasks",
			"This graph shows the number of successful tasks per 5min",
		)
		multigraph(
			tasks,
			task_started,
			"Started Tasks",
			GAUGE,
			"Started tasks per 5 min",
			"tasks",
			"This graph shows the number of tasks started per 5min",
		)
		multigraph(
			tasks,
			task_failed,
			"Failed Tasks",
			GAUGE,
			"Failed tasks per 5 min",
			"tasks",
			"This graph shows the number of failed tasks per 5min",
		)
		multigraph(
			tasks,
			task_received,
			"Received Tasks",
			GAUGE,
			"Tasks receivedper 5 min",
			"tasks",
			"This graph shows the number of tasks received per 5min",
		)
		multigraph(
			tasks,
			task_success_time,
			"Success Time",
			GAUGE,
			"Avg time to task success",
			"tasks",
			"This graph shows the average time to completion for successful tasks",
		)
		multigraph(
			tasks,
			task_failed_time,
			"Failure Time",
			GAUGE,
			"Avg time to task failure",
			"tasks",
			"This graph shows the average time to fail for tasks",
		)
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
	if len(queues) > 1 {
		multigraph(
			queues,
			queue_length,
			"Queue Length",
			GAUGE,
			"Queue Length",
			"queues",
			"This graph shows queue sizes over time",
		)
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
		err = yaml.Unmarshal(b, data)
	} else {
	}

	task_data, _ := data["tasks"].(map[interface {}]interface {})
	queue_data, _ := data["queues"].(map[interface {}]interface {})

	for _, task := range tasks {
		td, _ := task_data[task].(map[interface {}] interface{})
		fmt.Printf("%v %v\n", parent(task_received, task), get_int(td["num_received"]))
		fmt.Printf("%v %v\n", parent(task_started, task), get_int(td["num_started"]))
		fmt.Printf("%v %v\n", parent(task_failed, task), get_int(td["num_failed"]))
		fmt.Printf("%v %v\n", parent(task_success, task), get_int(td["num_success"]))
		fmt.Printf("%v %f\n", parent(task_start_time, task), get_float(td["start_time"]))
		fmt.Printf("%v %f\n", parent(task_success_time, task), get_float(td["success_time"]))
		fmt.Printf("%v %f\n", parent(task_failed_time, task), get_float(td["failure_time"]))
	}

	for _, queue := range queues {
		qd, _ := queue_data[queue].(map[interface {}] interface{})
		fmt.Printf("%v %v\n", parent(task_received, queue), get_int(qd["num_received"]))
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
	if len(os.Args) > 1 && os.Args[1] == "config" {
		configure(tasks, queues)
	} else {
		output(tasks, queues)
	}
}
