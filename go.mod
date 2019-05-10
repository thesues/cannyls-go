module github.com/thesues/cannyls-go

go 1.12

require (
	github.com/codegangsta/inject v0.0.0-20150114235600-33e0aa1cb7c0 // indirect
	github.com/codegangsta/martini v0.0.0-20170121215854-22fa46961aab // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/gin-gonic/gin v1.4.0
	github.com/google/btree v1.0.0
	github.com/klauspost/readahead v1.3.0
	github.com/kr/pretty v0.1.0 // indirect
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pkg/errors v0.8.1
	github.com/satori/go.uuid v1.2.0
	github.com/stretchr/testify v1.3.0
	github.com/thesues/go-judy v0.1.0
	github.com/urfave/cli v1.20.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
)

replace github.com/thesues/go-judy v0.1.0 => ./go-judy
