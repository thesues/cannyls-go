all:
	go test ./...
profile:
	cd storage ; go test -bench . -cpuprofile cpuprofile.out -memprofile memprofile.out
viewprofile:
	cd storage; pprof -http=:8080 cpuprofile.out