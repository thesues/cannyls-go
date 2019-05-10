all:test
build:
	git submodule update --init
	cd go-judy && make
	cd cmd/kanils && go build
	cd cmd/readup && go build
test:build
	go test ./...
profile:build
	cd storage ; go test -bench . -cpuprofile cpuprofile.out -memprofile memprofile.out
viewprofile:build
	cd storage; pprof -http=:8080 cpuprofile.out