module github.com/thesues/cannyls-go

go 1.12

require (
	github.com/dustin/go-humanize v1.0.0
	github.com/gin-contrib/static v0.0.0-20190511124741-c1cdf9c9ec7b
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
