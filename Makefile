export CC=clang

all: local linux64

local: dependencies
	go build -o build/local/monitor -a src/monitor.go
	go build -o build/local/munin -a src/munin.go

linux64: dependencies
	export GOOS=linux
	export GOARCH=amd64
	go build -o build/linux64/monitor -a src/monitor.go
	go build -o build/linux64/munin -a src/munin.go

# fetch the libs
dependencies:
	go get monitor
	go get munin
