package main

import (
	"fmt"

	"errors"
	"io"
	"os"
	"strings"
	"time"

	"math/rand"

	"github.com/dustin/go-humanize"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/storage"
	"github.com/urfave/cli"
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

func printHeader(header nvm.StorageHeader) {
	fmt.Println("===cannyls header===")
	fmt.Printf("UUID  %v\n", header.UUID)
	fmt.Printf("Block Size %d \n", header.BlockSize.AsU16())
	fmt.Printf("Version %d %d \n", header.MajorVersion, header.MinorVersion)
	fmt.Printf("Journal Region Size %d, for short %s\n", header.JournalRegionSize, humanize.Bytes(header.JournalRegionSize))
	fmt.Printf("Data    Region Size %d, for short %s\n", header.DataRegionSize, humanize.Bytes(header.DataRegionSize))
}

func printUsage(usage storage.StorageUsage) {
	fmt.Printf("===cannyls usage===")
	fmt.Printf("file counts %v\n", usage.FileCounts)
	fmt.Printf("data capacity %s\n", humanize.Bytes(usage.DataCapacity))
	fmt.Printf("data Free Bytes %s \n", humanize.Bytes(usage.DataFreeBytes))
	fmt.Printf("journal capacity %s \n", humanize.Bytes(usage.JournalCapacity))
	fmt.Printf("journal Usage Bytes %s \n", humanize.Bytes(usage.JournalUsageBytes))

}

func headerCannyls(c *cli.Context) (err error) {
	replay := c.Bool("replay")
	path := c.String("storage")

	fmt.Println(replay)
	//do not restore index
	if replay == false {
		fileNVM, header, err := nvm.Open(path)
		if err != nil {
			return err
		}
		fileNVM.Close()
		printHeader(*header)
		return err
	}

	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	header := store.Header()
	printHeader(header)
	printUsage(store.Usage())
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
	if _, _, err = store.Delete(id); err != nil {
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

func wbenchCannyls(c *cli.Context) (err error) {
	return benchCannyls(c, false)
}

func wrbenchCannyls(c *cli.Context) (err error) {
	return benchCannyls(c, true)
}

/*
./kanils WRBench --storage /mnt/catcannyls/4kbench.lusf --size 4096 --count 549755813
total = {2TB}Byte, elapsed = {14h18m17.387808585s}
takes 61GB
throughput :40 + 60 MB/s
iops :9K
*/
func benchCannyls(c *cli.Context, read bool) (err error) {

	count := c.Uint64("count")
	size := c.Uint64("size")
	always := c.BoolT("always")
	sync := c.BoolT("sync")

	fmt.Printf("Benching cannyls : embed data?:%v, sync?: %v\n", !always, sync)
	if count == 0 || size == 0 {
		return errors.New("argu count or size is zero")
	}
	store, err := createCannylsForBench(c)
	if err != nil {
		return
	}
	defer store.Close()
	var i uint64
	start := time.Now()
	marching := 100
	m := 0
	keystore := make([]lump.LumpId, marching)
	for i = 0; i < count; i++ {
		id := lump.FromU64(0, uint64(i))
		data := fillData(int(size))
		if size > (4<<10) || always {
			if _, err = store.Put(id, data); err != nil {
				return
			}
		} else {
			if _, err = store.PutEmbed(id, data.AsBytes()); err != nil {
				return
			}
		}

		if read {
			if m < marching {
				keystore[m] = id
				m++
			} else {
				n := 0
				for _, i := range keystore {
					if rand.Float64() < 0.5 {
						if _, _, err = store.Delete(i); err != nil {
							return err
						}
						n += 1
					}
				}
				m = 0
				//fmt.Printf("done %d\n", n)
			}
		}

		if sync {
			store.Sync()
		}
	}

	fmt.Printf("total = {%s}Byte, elapsed = {%+v}\n", bytesToString(uint64(size)*uint64(count)), time.Now().Sub(start))
	return
}

func createCannylsForBench(c *cli.Context) (store *storage.Storage, err error) {

	size := c.Uint64("size")
	count := c.Uint64("count")
	path := c.String("storage")

	capacityBytes := block.Min().CeilAlign(size * count * 8 / 10)

	fmt.Printf("create cannyls... capacity is %s\n", bytesToString(capacityBytes))
	store, err = storage.CreateCannylsStorage(path, capacityBytes, 0.1)
	if err != nil {
		return
	}

	return
}

func getCannyls(c *cli.Context) (err error) {
	path := c.String("storage")
	store, err := storage.OpenCannylsStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()

	key := c.Uint64("key")
	id := lump.FromU64(0, key)
	data, err := store.Get(id)
	if err != nil {
		return
	}
	fmt.Printf("value: %s", string(data))
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
			Name:  "Create",
			Usage: "Create --storage <path> --capacity <size>",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "capacity"},
			},
			Action: createCannyls,
		},
		{
			Name:  "Put",
			Usage: "Put --storage path --key key --value value",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "key"},
				cli.StringFlag{Name: "value"},
			},
			Action: putCannyls,
		},
		{
			Name:  "Get",
			Usage: "Get --storage path --key key",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "key"},
			},
			Action: getCannyls,
		},
		{
			Name:  "Dump",
			Usage: "Dump --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
			},
			Action: dumpCannyls,
		},
		{
			Name:  "Delete",
			Usage: "Delete --storage path --key key",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "key"},
			},
			Action: deleteCannyls,
		},
		{
			Name:  "Journal",
			Usage: "Journal --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
			},
			Action: journalCannyls,
		},
		{
			Name:  "JournalGC",
			Usage: "JournalGC --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
			},
			Action: journalGCCannyls,
		},
		{
			Name:  "Header",
			Usage: "Header --storage path --replay <true> ",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.BoolTFlag{Name: "replay"},
			},
			Action: headerCannyls,
		},
		{
			Name:  "WBench",
			Usage: "WBench --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "count"},
				cli.Uint64Flag{Name: "size"},
				cli.BoolTFlag{Name: "always"},
				cli.BoolTFlag{Name: "sync"},
			},
			Action: wbenchCannyls,
		},
		{
			Name:  "WRBench",
			Usage: "WRBench --storage path",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "storage"},
				cli.Uint64Flag{Name: "count"},
				cli.Uint64Flag{Name: "size"},
				cli.BoolTFlag{Name: "always"},
				cli.BoolTFlag{Name: "sync"},
			},
			Action: wrbenchCannyls,
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}

func fillData(size int) lump.LumpData {
	lumpData := lump.NewLumpDataAligned(size, block.Min())
	buf := lumpData.AsBytes()
	for i := 0; i < len(buf); i++ {
		buf[i] = 'A'
	}
	return lumpData
}

func bytesToString(size uint64) string {
	//KB, MB, GB, TB
	units := []string{"B", "KB", "MB", "GB", "TB"}
	i := size
	s := 0
	for i > 1024 {
		if s == len(units)-1 {
			break
		}
		i = i >> 10
		s++
	}

	return fmt.Sprintf("%d%s", i, units[s])
}
