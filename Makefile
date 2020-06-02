all:test
build:
	cd cmd/kanils && go build
	cd cmd/readup && go build
test:build
	go test ./... -race -coverprofile=coverage.txt -covermode=atomic
profile:build
	cd storage ; go test -bench . -cpuprofile cpuprofile.out -memprofile memprofile.out
viewprofile:build
	cd storage; pprof -http=:8080 cpuprofile.out
