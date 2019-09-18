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

	"github.com/gin-contrib/static"
	"github.com/thesues/cannyls-go/lump"
	x "github.com/thesues/cannyls-go/metrics"
	"github.com/thesues/cannyls-go/storage"
	"github.com/thesues/cannyls-go/util"
	"github.com/urfave/cli"

	"context"
	"net/http"
	"time"
)

//https://gist.github.com/davealbert/6278ecbdf679c755f29bab5d325e2995
func favicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/x-icon")
	w.Header().Set("Cache-Control", "public, max-age=7776000")
	fmt.Fprintln(w, "data:image/x-icon;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQEAYAAABPYyMiAAAABmJLR0T///////8JWPfcAAAACXBIWXMAAABIAAAASABGyWs+AAAAF0lEQVRIx2NgGAWjYBSMglEwCkbBSAcACBAAAeaR9cIAAAAASUVORK5CYII=")
}

//Based on https://medium.com/@cep21/how-to-correctly-use-context-context-in-go-1-7-8f2c0fafdf39

//getalloc is not thread-safe.
type GetAllocRequest struct {
	resultChan chan []float64
}

//put
type PutRequest struct {
	ctx        context.Context
	data       lump.LumpData
	id         uint64
	isAutoId   bool
	resultChan chan PutResult
}

type PutResult struct {
	id  uint64
	err error
}

//get
type GetRequest struct {
	ctx        context.Context
	id         uint64
	resultChan chan GetResult
	httpRange *HttpRange
}

type GetResult struct {
	data []byte
	err  error
}

//delete
type DeleteRequest struct {
	ctx        context.Context
	resultChan chan GetResult
}

var (
	TimeoutError       = errors.New("process timeout")
	NoKeyError         = errors.New("no more files")
	NoLumpIdSpaceError = errors.New("all lumpID is Used")
)

func chooseID(store *storage.Storage) (lump.LumpId, bool) {
	return store.GenerateEmptyId()
}

func handleGetRequest(store *storage.Storage, request GetRequest) {
	var response GetResult
	select {
	case <-request.ctx.Done():
		//timeout
		response.err = TimeoutError
		request.resultChan <- response
		return
	default:
	}
	var id lump.LumpId = lump.FromU64(0, request.id)

	var err error
	if request.httpRange == nil {
		response.data, err = store.Get(id)
	} else {
		response.data, err = store.GetWithOffset(id, uint32(request.httpRange.OffsetBegin),
				 uint32(request.httpRange.GetLength()))
	}
	if err != nil {
		response.err = err
	}

	select {
	//timeout
	case <-request.ctx.Done():
		request.resultChan <- GetResult{data: nil, err: TimeoutError}
	case request.resultChan <- response:
	}
}

func handleRandomRequest(store *storage.Storage, request DeleteRequest) {

	var response GetResult
	var err error
	select {
	case <-request.ctx.Done():
		//timeout
		response.err = TimeoutError
		request.resultChan <- response
		return
	default:
	}

	id, have := store.MinId()
	if have == false {
		response.data = nil
		response.err = NoKeyError
		goto END
	}
	response.data, err = store.Get(id)
	if err != nil {
		response.err = err
		goto END
	}

	_, _, err = store.Delete(id)
	if err != nil {
		response.err = err
	}

END:
	select {
	//timeout
	case <-request.ctx.Done():
		request.resultChan <- GetResult{data: nil, err: TimeoutError}
	case request.resultChan <- response:
	}
}

func handleAllocRequest(store *storage.Storage, request GetAllocRequest) {
	vec := store.GetAllocationStatus()
	request.resultChan <- vec
}

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
			request.resultChan <- PutResult{id.U64(), NoLumpIdSpaceError}
			return
		}
	}

	_, err := store.Put(id, request.data)
	response.id = id.U64()
	if err != nil {
		response.err = err
	}
	store.JournalSync()

	select {
	//timeout
	case <-request.ctx.Done():
		request.resultChan <- PutResult{id: id.U64(), err: TimeoutError}
	case request.resultChan <- response:
	}

}

func ServeStore(store *storage.Storage) {
	fmt.Printf("start http server\n")

	reqeustChan := make(chan interface{}, 10)

	//cannyls storage routine
	cannylsStopper := util.NewStopper()

	cannylsStopper.RunWorker(func() {
		for {
			select {
			case <-cannylsStopper.ShouldStop():
				store.Close()
				return
			case request := <-reqeustChan:
				switch request.(type) {
				case PutRequest:
					handlePutRequest(store, request.(PutRequest))
				case GetRequest:
					handleGetRequest(store, request.(GetRequest))
				case DeleteRequest:
					handleRandomRequest(store, request.(DeleteRequest))
				case GetAllocRequest:
					handleAllocRequest(store, request.(GetAllocRequest))
				}
			case <-time.After(3 * time.Second):
				store.RunSideJobOnce()
			}
		}
	})

	r := gin.Default()

	//call delete
	r.GET("/random", func(c *gin.Context) {
		ctx := context.Background()
		request := DeleteRequest{
			ctx:        ctx,
			resultChan: make(chan GetResult),
		}

		reqeustChan <- request

		select {
		case out := <-request.resultChan:
			if out.err != nil {
				c.String(400, out.err.Error())
			} else {
				c.Status(200)
				c.Header("content-length", fmt.Sprintf("%d", len(out.data)))
				c.Stream(func(w io.Writer) bool {
					_, err := w.Write(out.data)
					if err != nil {
						return true
					}
					return false
				})
				return
			}
		case <-ctx.Done():
			c.String(400, "TIMEOUT")
		}
	})

	r.Use(static.Serve("/static", static.LocalFile("./static", false)))
	r.GET("/getalloc/", func(c *gin.Context) {
		//I am lazy, no timeout here
		resultChan := make(chan []float64)
		reqeustChan <- GetAllocRequest{resultChan: resultChan}

		out := <-resultChan
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
		ctx := context.Background()
		
		var httpRange *HttpRange = nil
		if c.GetHeader("Range") != "" { 
			httpRange, err = ParseRequestRange(c.GetHeader("Range"), lump.LUMP_MAX_SIZE)
			if err != nil {
				c.String(400, err.Error())
			}
		}
		
		request := GetRequest{
			ctx:        ctx,
			id:         uint64(id),
			resultChan: make(chan GetResult),
			httpRange: httpRange,
		}
		
		

		reqeustChan <- request

		select {
		case out := <-request.resultChan:
			if out.err != nil {
				c.String(400, out.err.Error())
			} else {
				c.Status(200)
				c.Header("content-length", fmt.Sprintf("%d", len(out.data)))
				c.Stream(func(w io.Writer) bool {
					_, err := w.Write(out.data)
					if err != nil {
						return true
					}
					return false
				})
				return
			}
		case <-ctx.Done():
			c.String(400, "TIMEOUT")
		}
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

		/*
		parse the http header first, and send 100-continue
		if the uploaded file's size is LUMP_MAX_SIZE, but the
		content length will be bigger in form.
		so, less than 30MB will be fine.
		*/
		if c.Request.ContentLength > lump.LUMP_MAX_SIZE {
			c.String(413 , "size too big")
			return
		}
		
		//parse the real data
		readFile, header, err := c.Request.FormFile("file")
		if err != nil {
			c.String(400, err.Error())
			return
		}
		//ab := lump.NewLumpDataAligned(int(header.Size), block.Min())
		//alloc ab
		ab := lump.GetLumpData(int(header.Size))
		defer lump.PutLumpData(ab)

		_, err = io.ReadFull(readFile, ab.AsBytes())
		if err != nil {
			c.String(409, "read failed")
			return
		}

		ctx := context.Background()
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
	app.Action = func(c *cli.Context) {
		storagePath := c.String("storage")
		store, err := storage.OpenCannylsStorage(storagePath)
		if err != nil {
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
