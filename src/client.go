package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

type GraphType string

const (
	GAUGE = GraphType("GAUGE")
	COUNTER = GraphType("COUNTER")
	DERIVE = GraphType("DERIVE")
)

func undot(s string) string {
	return strings.Replace(s, ".", "_", -1)
}

// prepends the parent, and undots the item
func parent(parent string, item string) string {
	return fmt.Sprintf("%v_%v", parent, undot(item))
}

func Map(src []string, f func(string)string) []string {
	dst := make([]string, 0, len(src))
	for _, s := range src {
		dst = append(dst, f(s))
	}
	return dst
}

func multigraph(entries []string, id string, name string, graph_type GraphType, vlabel string, category string, info string) {
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
			"task_success",
			"Successful Tasks",
			GAUGE,
			"Successful tasks per 5 min",
			"celery_tasks",
			"This graph shows the number of successful tasks per 5min",
		)
		multigraph(
			tasks,
			"task_started",
			"Started Tasks",
			GAUGE,
			"Started tasks per 5 min",
			"celery_tasks",
			"This graph shows the number of tasks started per 5min",
		)
		multigraph(
			tasks,
			"task_failed",
			"Failed Tasks",
			GAUGE,
			"Failed tasks per 5 min",
			"celery_tasks",
			"This graph shows the number of failed tasks per 5min",
		)
		multigraph(
			tasks,
			"task_received",
			"Received Tasks",
			GAUGE,
			"Tasks receivedper 5 min",
			"celery_tasks",
			"This graph shows the number of tasks received per 5min",
		)
		multigraph(
			tasks,
			"task_success_time",
			"Success Time",
			GAUGE,
			"Avg time to task success",
			"celery_tasks",
			"This graph shows the average time to completion for successful tasks",
		)
		multigraph(
			tasks,
			"task_failed_time",
			"Failure Time",
			GAUGE,
			"Avg time to task failure",
			"celery_tasks",
			"This graph shows the average time to fail for tasks",
		)
		multigraph(
			tasks,
			"task_start_time",
			"Start Time",
			GAUGE,
			"Avg time to start task",
			"celery_tasks",
			"This graph shows the average time it takes a task to start after it's received",
		)
	}
	if len(queues) > 1 {
		multigraph(
			queues,
			"queue_length",
			"Queue Length",
			GAUGE,
			"Queue Length",
			"celery_tasks",
			"This graph shows queue sizes over time",
		)
	}


}

// prints metrics for the tasks
func output(tasks []string, queues []string) {
	data_path := os.Getenv("data_path")
	if data_path == "" { data_path = "/tmp/celerymunin.out" }

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

func main() {
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
