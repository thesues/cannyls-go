package main

import (
	"fmt"

	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/storage"
	"github.com/urfave/cli"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"context"
	"io"
	"net/http"
	"time"
)

func doServer(store *storage.Storage) {
	var err error

	fmt.Printf("start http server\n")

	sc := make(chan os.Signal, 1)

	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		select {
		case sig := <-sc:
			// send signal again, return directly
			fmt.Printf("\nGot signal [%v] to exit.\n", sig)
			os.Exit(1)
		}
	}()

	//https://medium.com/@cep21/how-to-correctly-use-context-context-in-go-1-7-8f2c0fafdf39

	type Result struct {
		data []byte
	}

	type Request struct {
		ctx        context.Context
		resultChan chan Result
	}

	reqeustChan := make(chan Request, 10)

	go func() {
		for {
			select {
			case request := <-reqeustChan:
				select {
				case <-request.ctx.Done():
					continue
				default:
				}

				var result Result
				id, have := store.MinId()
				if have == false {
					result.data = nil
				}
				result.data, err = store.Get(id)
				if err != nil {
					result.data = nil
				}
				store.Delete(id)

				select {
				case <-request.ctx.Done():
				case request.resultChan <- result:
				}
			case <-time.After(3 * time.Second):
				store.RunSideJobOnce()
			}
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		request := Request{
			ctx:        ctx,
			resultChan: make(chan Result),
		}

		reqeustChan <- request

		select {
		case out := <-request.resultChan:
			if out.data != nil {
				w.WriteHeader(200)
				w.Write(out.data)
			} else {
				w.WriteHeader(404)
				w.Write([]byte("<html>404 no more images</html>"))
			}

		case <-ctx.Done():
			w.WriteHeader(405)
		}

	})

	http.ListenAndServe(":8081", nil)
	return

}
func doInsertAndServe(c *cli.Context) (err error) {

	storagePath := c.String("storage")
	downloadImagesPath := c.String("imagespath")

	store, err := storage.OpenCannylsStorage(storagePath)
	if err != nil {
		return err
	}
	defer store.Close()

	if downloadImagesPath == "" {
		doServer(store)
		return nil
	}

	if false == strings.HasSuffix(downloadImagesPath, "/") {
		downloadImagesPath += "/"
	}
	fmt.Println(downloadImagesPath)
	files, err := filepath.Glob(downloadImagesPath + "*")
	if err != nil {
		return
	}
	var processedFile = 0
	for _, f := range files {
		fd, err := os.Open(f)
		if err != nil {
			return err
		}
		ids := strings.Split(f, "_")
		id, err := strconv.ParseUint(ids[1], 10, 64)
		if err != nil {
			fmt.Printf("can not convert %s\n", f)
			continue
		}

		info, err := fd.Stat()
		if err != nil {
			fmt.Printf("can not find size for %s\n", f)
			continue
		}
		if info.Size() > lump.LUMP_MAX_SIZE {
			fmt.Printf("size is too big %d\n", info.Size())
			continue
		}

		ab := lump.NewLumpDataAligned(int(info.Size()), block.Min())

		io.ReadFull(fd, ab.AsBytes())
		updated, err := store.Put(lumpidnum(id), ab)
		fmt.Printf("insert %s, duplicated ? %v\n", f, updated)
		fd.Close()
		processedFile++
	}

	fmt.Printf("Processed %d files\n", processedFile)
	return

}

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "storage"},
		cli.StringFlag{Name: "imagespath"},
	}
	app.Action = doInsertAndServe

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}

func lumpidnum(n uint64) lump.LumpId {
	l := lump.FromU64(0, n)
	return l
}
