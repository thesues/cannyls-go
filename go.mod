module github.com/thesues/cannyls-go

go 1.12

require (
	github.com/dustin/go-humanize v1.0.0
	github.com/google/btree v1.0.0
	github.com/google/pprof v0.0.0-20190404155422-f8f10df84213 // indirect
	github.com/ianlancetaylor/demangle v0.0.0-20181102032728-5e5cf60278f6 // indirect
	github.com/keegancsmith/rpc v1.1.0 // indirect
	github.com/klauspost/readahead v1.3.0
	github.com/mdempsky/gocode v0.0.0-20190203001940-7fb65232883f // indirect
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pkg/errors v0.8.1
	github.com/satori/go.uuid v1.2.0
	github.com/stretchr/testify v1.3.0
	github.com/thesues/go-judy v0.1.0
	github.com/urfave/cli v1.20.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
)

replace github.com/thesues/go-judy v0.1.0 => ./go-judy
