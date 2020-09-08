package main

import (
	"errors"
	"fmt"
	"strconv"

	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"go.opencensus.io/trace"

	"context"
	"net/http"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-contrib/static"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	x "github.com/thesues/cannyls-go/metrics"
	"github.com/thesues/cannyls-go/storage"
	"github.com/thesues/cannyls-go/util"
	"github.com/urfave/cli"
)

//https://gist.github.com/davealbert/6278ecbdf679c755f29bab5d325e2995
func favicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/x-icon")
	w.Header().Set("Cache-Control", "public, max-age=7776000")
	fmt.Fprintln(w, "data:image/x-icon;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQEAYAAABPYyMiAAAABmJLR0T///////8JWPfcAAAACXBIWXMAAABIAAAASABGyWs+AAAAF0lEQVRIx2NgGAWjYBSMglEwCkbBSAcACBAAAeaR9cIAAAAASUVORK5CYII=")
}

//Based on https://medium.com/@cep21/how-to-correctly-use-context-context-in-go-1-7-8f2c0fafdf39

var backupRuning bool = false

//put
type PutRequest struct {
	ctx        context.Context
	data       lump.LumpData
	id         uint64
	isAutoId   bool
	resultChan chan PutResult
}

type PutResult struct {
	id         uint64
	err        error
	resultChan chan PutResult
}

var (
	TimeoutError       = errors.New("process timeout")
	NoKeyError         = errors.New("no more files")
	NoLumpIdSpaceError = errors.New("all lumpID is Used")
)

func chooseID(store *storage.Storage) (lump.LumpId, bool) {
	return store.GenerateEmptyId()
}

/*
func handlePutRequest(store *storage.Storage, request PutRequest) {
	var response PutResult

	select {
	case <-request.ctx.Done():
		//timeout
		response.err = TimeoutError
		request.resultChan <- response
		return
	default:
	}

	var id lump.LumpId = lump.FromU64(0, request.id)
	var have bool
	if request.isAutoId {
		id, have = chooseID(store)
		if !have {
			request.resultChan <- PutResult{id.U64(), NoLumpIdSpaceError, }
			return
		}
	}

	_, err := store.Put(id, request.data)
	response.id = id.U64()
	if err != nil {
		response.err = err
	}
	//store.Sync()

	select {
	//timeout
	case <-request.ctx.Done():
		request.resultChan <- PutResult{id: id.U64(), err: TimeoutError}
	case request.resultChan <- response:
	}

}
*/

func ServeStore(store *storage.Storage) {
	fmt.Printf("start http server\n")

	reqeustChan := make(chan PutRequest, 20)

	//cannyls storage routine
	cannylsStopper := util.NewStopper()

	cannylsStopper.RunWorker(func() {
		for {
			select {
			case <-cannylsStopper.ShouldStop():
				store.Close()
				return
			case r := <-reqeustChan:
				var requests []PutRequest
			slurp_loop:
				for {
					requests = append(requests, r)
					select {
					case r = <-reqeustChan:
					default:
						break slurp_loop
					}
				}

				var results []PutResult

				fmt.Printf("len of slurp is %d\n", len(requests))
				for _, req := range requests {
					_, span := trace.StartSpan(req.ctx, "write cannyls")
					var id lump.LumpId = lump.FromU64(0, req.id)
					var have bool
					if req.isAutoId {
						id, have = chooseID(store)
						if !have {
							req.resultChan <- PutResult{id.U64(), NoLumpIdSpaceError, nil}
							span.End()
							return
						}
					}
					var err error
					span.Annotate(nil, "Cannyls: put data")
					/*
						if req.data.Inner.Len() < lump.MAX_EMBEDDED_SIZE {
							_, err = store.PutEmbed(id, req.data.Inner.AsBytes())
						} else {
					*/
					_, err = store.Put(id, req.data)
					//}
					span.Annotate(nil, "Cannyls: put data done")
					span.End()
					var result PutResult
					result.id = id.U64()
					if err != nil {
						result.err = err
					}
					result.resultChan = req.resultChan
					results = append(results, result)
				}

				var spans []*trace.Span
				for _, req := range requests {
					_, span := trace.StartSpan(req.ctx, "sync")
					spans = append(spans, span)
				}

				store.Flush()
				//store.Sync()
				for i := range spans {
					spans[i].End()
				}
				for _, result := range results {
					result.resultChan <- result
				}

			case <-time.After(3 * time.Second):
				store.RunSideJobOnce(64)
			}
		}
	})

	r := gin.Default()

	pprof.Register(r)

	r.GET("/usage", func(c *gin.Context) {
		c.JSON(200, store.Usage())
	})

	//call delete
	r.GET("/random", func(c *gin.Context) {
		id, have := store.MinId()
		if !have {
			c.String(404, "do not have")
			return
		}
		data, err := store.Get(id)
		if err != nil {
			c.String(500, err.Error())
			return
		}
		store.Delete(id)
		c.Status(200)
		c.Header("content-length", fmt.Sprintf("%d", len(data)))
		c.Stream(func(w io.Writer) bool {
			_, err := w.Write(data)
			if err != nil {
				fmt.Println(err)
				return true
			}
			return false
		})

	})

	r.Use(static.Serve("/static", static.LocalFile("./static", false)))

	r.GET("/getalloc/", func(c *gin.Context) {
		out := store.GetAllocationStatus()
		c.JSON(200, out)
	})

	r.GET("/metrics", func(c *gin.Context) {
		x.PrometheusHandler.ServeHTTP(c.Writer, c.Request)
	})

	r.GET("/get/:id", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.String(400, err.Error())
			return
		}

		data, err := store.Get(lump.FromU64(0, id))
		if err != nil {
			c.String(500, err.Error())
			return
		}
		c.Status(200)
		c.Header("content-length", fmt.Sprintf("%d", len(data)))
		c.Stream(func(w io.Writer) bool {
			_, err := w.Write(data)
			if err != nil {
				fmt.Println(err)
				return true
			}
			return false
		})
		return
	})

	r.POST("/snapshot/:op", func(c *gin.Context) {
		op := c.Param("op")
		var onlyCreateSnapshot bool
		switch op {
		case "backup":
			onlyCreateSnapshot = false
		case "create":
			onlyCreateSnapshot = true
		default:
			c.String(400, "input is invalid")
			return
		}

		if onlyCreateSnapshot {
			err := store.CreateSnapshot()
			if err != nil {
				c.String(500, err.Error())
				return
			}
			c.Status(200)
			return
		}

		if backupRuning {
			c.String(http.StatusCreated, "backup is running")
			return
		}

		backupStopper := util.NewStopper()
		backupStopper.RunWorker(func() {
			//open backup file, name is start time
			defer func() {
				backupRuning = false
			}()

			reader, err := store.GetSnapshotReader()
			fileName := time.Now().Format(time.RFC3339) + "_backup.lusf"
			backfile, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				return
			}
			var buf [512 << 10]byte
			for {
				select {
				case <-backupStopper.ShouldStop():
					backfile.Close()
					return
				case <-time.After(100 * time.Millisecond):
					n, err := reader.Read(buf[:])
					fmt.Printf("copy out data %d, %+v\n", n, err)
					if err != nil && err != io.EOF {
						return
					}
					if n == 0 {
						store.DeleteSnapshot()
						backfile.Close()
						return
					}
					n, err = backfile.Write(buf[0:n])
					if err != nil {
						return
					}
				}
			}
		})
	})

	r.POST("/put/*id", func(c *gin.Context) {
		var isAutoId = false
		var id uint64
		var err error
		sid := c.Param("id")
		//gin's optinal param always has a slas. check this https://github.com/gin-gonic/gin/issues/279
		if sid == "/" {
			isAutoId = true
		} else {
			id, err = strconv.ParseUint(sid[1:], 10, 64)
			if err != nil {
				c.String(400, err.Error())
				return
			}
		}

		readFile, header, err := c.Request.FormFile("file")
		if err != nil {
			c.String(400, err.Error())
			return
		}
		if header.Size > int64(lump.LUMP_MAX_SIZE) {
			c.String(405, "size too big")
			return
		}
		ab := lump.NewLumpDataAligned(int(header.Size), block.Min())
		_, err = io.ReadFull(readFile, ab.AsBytes())
		if err != nil {
			c.String(409, "read failed")
			return
		}

		//ctx := context.Background()

		ctx, span := trace.StartSpan(context.Background(), "PutRequest")
		defer span.End()
		span.Annotate(nil, "HTTP START")
		request := PutRequest{
			ctx:        ctx,
			data:       ab,
			id:         uint64(id),
			isAutoId:   isAutoId,
			resultChan: make(chan PutResult),
		}

		reqeustChan <- request

		select {
		case out := <-request.resultChan:
			span.Annotate(nil, "HTTP END")
			if out.err != nil {
				c.String(400, out.err.Error())
			} else {
				c.String(200, "The ID is %d\n", out.id)
				return
			}
		case <-ctx.Done():
			c.String(400, "TIMEOUT")
		}

	})

	srv := &http.Server{
		Addr:    ":8081",
		Handler: r,
	}

	httpStopper := util.NewStopper()
	httpStopper.RunWorker(func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic("http server crashed")
		}
	})

	sc := make(chan os.Signal, 1)

	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	for {
		select {
		case sig := <-sc:
			// send signal again, return directly
			if sig != syscall.SIGINT {
				continue
			}
			fmt.Printf("\nGot signal [%v] to exit.\n", sig)

			//stop http server
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(ctx); err != nil {
				panic("Server Shutdown failed")
			}
			httpStopper.Stop() //wait for the ListenAndServe finish.

			//stop cannyls server, it will call store.Close first.
			cannylsStopper.Stop()
			return
		}
		return
	}
}

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "storage"},
	}

	/*
			je, err := jaeger.NewExporter(jaeger.Options{
				Endpoint:    "http://localhost:14268",
				ServiceName: "readup",
			})
			trace.RegisterExporter(je)

		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

		if err != nil {
			panic(fmt.Sprintf("Failed to create the Jaeger exporter: %+v", err))
		}
	*/
	app.Action = func(c *cli.Context) {
		storagePath := c.String("storage")
		store, err := storage.OpenCannylsStorage(storagePath)
		if err != nil {
			fmt.Printf("failed to open %+v", err)
			return
		}
		ServeStore(store)
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}

func lumpidnum(n uint64) lump.LumpId {
	l := lump.FromU64(0, n)
	return l
}
