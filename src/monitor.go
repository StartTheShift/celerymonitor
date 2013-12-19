package main

import (
	"net/http"
	_ "expvar"
	_ "net/http/pprof"
)

import "monitor"

func main() {
	go http.ListenAndServe(":8080", nil)

	monitor.RunMonitor()
}

