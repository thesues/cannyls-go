package main

import (
	"fmt"

	"errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/storage"
	"github.com/urfave/cli"
	"io"
	"os"
	"strings"
)

func createCannyls(c *cli.Context) error {
	path := c.String("storage")
	capactiyBytes := c.Uint64("capacity")
	capactiyBytes = block.Min().CeilAlign(capactiyBytes)
	fmt.Printf("Creating cannyls <%s>, capacity is <%d>\n", path, capactiyBytes)
	store, err := storage.CreateCannylsStorage(path, capactiyBytes, 0.01)
	if err != nil {
		fmt.Printf("%+v\n", err)
		return err
	}
	store.Close()
	return nil
}

func headerCannyls(c *cli.Context) (err error) {
	path := c.String("storage")
	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	header := store.Header()
	fmt.Println("===cannyls header===")
	fmt.Printf("Block Size %d \n", header.BlockSize.AsU16())
	fmt.Printf("Version %d %d \n", header.MajorVersion, header.MinorVersion)
	fmt.Printf("Journal Region Size %d\n", header.JournalRegionSize)
	fmt.Printf("Data    Region Size %d\n", header.DataRegionSize)
	return
}

func deleteCannyls(c *cli.Context) (err error) {
	path := c.String("storage")
	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	key := c.Uint64("key")
	id := lump.FromU64(0, key)
	if _, err = store.Delete(id); err != nil {
		return err
	}

	fmt.Printf("id %s is deleted\n", id.String())
	return
}

func dumpCannyls(c *cli.Context) (err error) {
	path := c.String("storage")
	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	ids := store.List()
	for _, id := range ids {
		d, err := store.Get(id)
		if err != nil {
			break
		}

		fmt.Printf("%s :%s\n", id.String(), string(d))
	}
	return
}

func journalGCCannyls(c *cli.Context) (err error) {
	path := c.String("storage")
	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	store.JournalGC()
	fmt.Println("Journal Full GC completed")
	return
}

func journalCannyls(c *cli.Context) (err error) {
	path := c.String("storage")
	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	snap := store.JournalSnapshot()
	fmt.Printf("unused header :%d\n", snap.UnreleasedHead)
	fmt.Printf("header        :%d\n", snap.Head)
	fmt.Printf("tail          :%d\n", snap.Tail)

	for _, entry := range snap.Entries {
		fmt.Printf("<%+v>\n", entry)
	}
	return
}

func putCannyls(c *cli.Context) (err error) {
	path := c.String("storage")
	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	key := c.Uint64("key")
	id := lump.FromU64(0, key)
	var value string
	var lumpdata lump.LumpData
	value = c.String("value")

	isFile := false
	if strings.HasPrefix(value, "@") {
		isFile = true
		file, err := os.Open(value[1:])
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			return err
		}
		size := info.Size()
		if size > lump.LUMP_MAX_SIZE {
			return errors.New("file is too big")
		}
		lumpdata = lump.NewLumpDataAligned(int(size), block.Min())

		if err = readUpData(file, lumpdata); err != nil {
			return err
		}

	}

	if isFile {
		_, err = store.Put(id, lumpdata)
	} else {
		_, err = store.PutEmbed(id, []byte(value))
	}
	if err != nil {
		fmt.Printf("isFile :%v, err is %+v", isFile, err)
	}
	return
}

func readUpData(r io.Reader, lumpdata lump.LumpData) error {
	s := lumpdata.AsBytes()
	for {
		n, err := r.Read(s)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s = s[:n]
	}
}

func main() {

	app := cli.NewApp()
	app.Name = "kanils"
	app.Usage = "kanils subcommand"
	app.Commands = []cli.Command{
		{
			Name:  "create",
			Usage: "create cannyls",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "capacity"},
			},
			Action: createCannyls,
		},
		{
			Name:  "put",
			Usage: "put --storage path --key key --value value",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "key"},
				cli.StringFlag{Name: "value"},
			},
			Action: putCannyls,
		},
		{
			Name:  "dump",
			Usage: "dump --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
			},
			Action: dumpCannyls,
		},
		{
			Name:  "delete",
			Usage: "put --storage path --key key",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "key"},
			},
			Action: deleteCannyls,
		},
		{
			Name:  "journal",
			Usage: "journal --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
			},
			Action: journalCannyls,
		},
		{
			Name:  "gc",
			Usage: "gc --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
			},
			Action: journalGCCannyls,
		},
		{
			Name:  "header",
			Usage: "header --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
			},
			Action: headerCannyls,
		},
	}
	app.Run(os.Args)
}
