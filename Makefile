export CC=clang

all: local linux64

local: dependencies
	go build -o build/local/celerymonitor -a src/monitor.go
	go build -o build/local/celerymunin -a src/munin.go

linux64: dependencies
	GOOS=linux GOARCH=amd64 go build -o build/linux64/celerymonitor -a src/monitor.go
	GOOS=linux GOARCH=amd64 go build -o build/linux64/celerymunin -a src/munin.go

# fetch the libs
dependencies:
	go get monitor
	go get munin
