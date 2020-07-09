[![Build Status](https://travis-ci.org/thesues/cannyls-go.svg?branch=master)](https://travis-ci.org/thesues/cannyls-go)
[![codecov](https://codecov.io/gh/thesues/cannyls-go/branch/master/graph/badge.svg)](https://codecov.io/gh/thesues/cannyls-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/thesues/cannyls-go)](https://goreportcard.com/report/github.com/thesues/cannyls-go)

# Cannyls-go

Cannyls-go is golang re-implenment for cannyls(https://github.com/frugalos/cannyls)


# Build

## Build requires

1. gcc
2. make
3. golang >= go1.12

Run make in the top directory, It will test all the modules first, and compile two
command tools

```
make
```


# Component

cmd/kanils


A comman line tool for cannyls, it is alternaitve to https://github.com/frugalos/kanils


cmd/readup

A HTTP server with cannyls-go as storage backend. HTTP API could be used to upload/delete data from cannyls-go



## Main differences bewteen origin cannyls

1. lumpid is 64bit, not 128bit
2. Origin cannyls use native rust standard library btreemap, cannyls-go uses libjudy(http://judy.sourceforge.net/) as index to 
save more memory.
3. Origin cannyls has a deadline schedule queue. Cannyls-go uses golang channel, leave it for user to implement its own strategy


## Benchmark


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fthesues%2Fcannyls-go.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fthesues%2Fcannyls-go?ref=badge_large)
