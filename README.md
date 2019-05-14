# cannyls-go

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

1. lumpid is 64bit, not 126bit
2. Origin cannyls use native rust standard library btreemap, cannyls-go uses libjudy(http://judy.sourceforge.net/) as index to 
save more memory.
3. Origin cannyls has a deadline schedule queue. Cannyls-go uses golang channel, leave it for user to implement its own strategy


## Benchmark

