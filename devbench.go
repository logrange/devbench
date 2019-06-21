package main

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/client"
	"github.com/logrange/range/pkg/transport"
	"github.com/logrange/range/pkg/utils/bytes"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Println("Usage: devbench <# of clients> <# of partitions> <rec size> <records>")
		os.Exit(1)
	}

	threads := parseInt(os.Args[2])
	rsize := parseInt(os.Args[3])
	count := parseInt(os.Args[4])
	clnts := createClients(parseInt(os.Args[1]))
	ctx := context.Background()
	writeBench(ctx, clnts, threads, rsize, count)
	readBench(ctx, clnts, threads, rsize, count)
	defer closeClients(clnts)
}

func createClients(clients int) []api.Client {
	res := make([]api.Client, clients)
	var err error
	for i := 0; i < clients; i++ {
		res[i], err = client.NewClient(transport.Config{
			ListenAddr: "127.0.0.1:9966",
		})

		if err != nil {
			fmt.Println("Could not create new client, err=", err)
			os.Exit(1)
		}
	}
	return res
}

func closeClients(clnts []api.Client) {
	for _, c := range clnts {
		c.Close()
	}
}

func parseInt(str string) int {
	i64, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		fmt.Println("Could not parse ", str, " as integer, err=", err)
		os.Exit(1)
	}

	if i64 < 1 {
		fmt.Println("Expecting positive value ", i64)
		os.Exit(1)
	}

	return int(i64)
}

func writeBench(ctx context.Context, clients []api.Client, threads, rsize, count int) error {
	start := time.Now()

	var wg sync.WaitGroup

	rand.Seed(time.Now().UnixNano())
	for i, c := range clients {
		wg.Add(1)
		go func (c api.Client, pstart int ) {
			writeRecs(ctx, c, threads, rsize, count, pstart)
			wg.Done()
		}(c, i)
	}
	wg.Wait()

	lng := time.Now().Sub(start)
	size := uint64(count*rsize*len(clients)*threads)
	perSec := uint64(float64(size)/(float64(lng)/float64(time.Second)))

	fmt.Println(humanize.Bytes(size), " were wrtitten for ", lng, " which is ", humanize.Bytes(perSec), "/s")

	return nil
}

func writeRecs(ctx context.Context, c api.Client, threads, rsize, count, pstart int) error {
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(tags string) {
			var wr api.WriteResult
			recs := makeTestEvents(rsize)

			for i := 0; i < count; i += len(recs) {
				err := c.Write(ctx, tags, "", recs, &wr)
				if err != nil {
					fmt.Println("oops, got the error ", err)
					break
				}
				if i%500000 == 0 {
					fmt.Println(tags, " written ", i, " records")
				}
				updateTS(recs)
			}
			wg.Done()
		}(fmt.Sprintf("test=hello%d", pstart + i))
	}
	wg.Wait()

	return nil
}

func makeTestEvents(size int) []*api.LogEvent {
	res := make([]*api.LogEvent, 0, 2000)
	buf := make([]byte, size)
	for i := 0; i < cap(res); i++ {
		res = append(res, &api.LogEvent{
			Timestamp: time.Now().UnixNano(),
			Message:   bytes.ByteArrayToString(buf),
		})
	}
	return res
}

func updateTS(recs []*api.LogEvent) {
	ts := time.Now().UnixNano()
	for _, r := range recs {
		r.Timestamp = ts
		ts++
	}
}

func readBench(ctx context.Context, clients []api.Client, threads, rsize, count int) error {
	start := time.Now()

	var wg sync.WaitGroup

	rand.Seed(time.Now().UnixNano())
	for i, c := range clients {
		wg.Add(1)
		go func (c api.Client, pstart int ) {
			readRecs(ctx, c, threads, rsize, count, pstart)
			wg.Done()
		}(c, i)
	}
	wg.Wait()

	lng := time.Now().Sub(start)
	size := uint64(count*rsize*len(clients)*threads)
	perSec := uint64(float64(size)/(float64(lng)/float64(time.Second)))

	fmt.Println(humanize.Bytes(size), " were read for ", lng, " which is ", humanize.Bytes(perSec), "/s")

	return nil
}

func readRecs(ctx context.Context, c api.Client, threads, rsize, count, pstart int) error {
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(tags string) {
			lc := count
			ctx1, cancel := context.WithCancel(ctx)
			api.Select(ctx1, c, &api.QueryRequest{Query: "select from " + tags + " position head limit 1000", Limit: 1000}, true, func(res *api.QueryResult) {
				lc -= len(res.Events)
				if lc <= 0 {
					cancel()
				}
				if lc%500000 == 0 {
					fmt.Println("left ", lc, res.Events[0].Timestamp)
				}
			})
			wg.Done()
		}(fmt.Sprintf("test=hello%d", pstart + i))
	}
	wg.Wait()

	return nil
}