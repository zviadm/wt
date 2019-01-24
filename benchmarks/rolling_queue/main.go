package main

import (
	"context"
	//"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"sync/atomic"
	"time"

	"wt"
)

func makeKey(i int) []byte {
	return []byte(fmt.Sprintf("key%20d", i))
}

func makeValue(rr *rand.Rand) []byte {
	v := make([]byte, 237)
	_, _ = rr.Read(v)
	return v
}

func inserter(ctx context.Context, c *wt.Connection, totalN int, rowCounter *int64) error {
	s, err := c.OpenSession(nil)
	if err != nil {
		return err
	}
	m, err := s.Mutate("table:test1", nil)
	if err != nil {
		return err
	}
	rr := rand.New(rand.NewSource(77))
	t0 := time.Now()
	prevI := 0
	progressN := 1000000
	for i := 0; ; i++ {
		key := makeKey(i)
		value := makeValue(rr)
		if err := m.Insert(key, value); err != nil {
			return err
		}
		rowCount := atomic.AddInt64(rowCounter, 1)

		if i%progressN == 0 {
			if ctx.Err() != nil {
				break
			}
			now := time.Now()
			log.Printf(
				"inserts: %v (%v), Queue mode? %v, per item: %v",
				i, rowCount, int64(i) > rowCount, now.Sub(t0)/time.Duration(i-prevI+1))
			prevI = i + 1
			t0 = now
		}
		for rowCount > int64(totalN+2*progressN) {
			time.Sleep(time.Millisecond)
		}
	}
	_ = m.Close()
	_ = s.Close()
	return nil
}

func rollingQueue(
	dbPath string, totalN int, useSnappy bool) error {
	dbPath = path.Join(dbPath, "bench_rollingqueue.wt")
	_ = os.RemoveAll(dbPath)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}
	c, err := wt.Open(dbPath, &wt.ConnectionConfig{Create: wt.True})
	if err != nil {
		return err
	}
	defer c.Close()
	s, err := c.OpenSession(nil)
	if err != nil {
		return err
	}
	tableCfg := &wt.DataSourceConfig{}
	if useSnappy {
		tableCfg.BlockCompressor = "snappy"
	}
	if err := s.Create("table:test1", tableCfg); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	inserterDone := make(chan error, 1)
	defer func() {
		cancel()
		<-inserterDone
	}()
	var rowCounter int64
	go func() {
		inserterDone <- inserter(ctx, c, totalN, &rowCounter)
	}()

	m, err := s.Mutate("table:test1", nil)
	if err != nil {
		return err
	}
	scanner, err := s.Scan("table:test1")
	if err != nil {
		return err
	}
	for {
		if atomic.LoadInt64(&rowCounter) < int64(totalN) {
			log.Print("Remover: Caughtup, sleeping......")
			time.Sleep(time.Second)
			for atomic.LoadInt64(&rowCounter) < int64(totalN) {
				time.Sleep(time.Second)
			}
			log.Print("Remover: Back at it......")
		}
		if err := scanner.Next(); err != nil {
			return err
		}
		key, err := scanner.UnsafeKey()
		if err != nil {
			return err
		}
		if err := m.Remove(key); err != nil {
			return err
		}
		atomic.AddInt64(&rowCounter, -1)
	}

	if err := m.Close(); err != nil {
		return err
	}
	if err := scanner.Close(); err != nil {
		return err
	}
	if err := s.Close(); err != nil {
		return err
	}
	return nil
}

func main() {
	dbPath := flag.String("path", "/tmp", "")
	totalN := flag.Int("total_items", 10000, "")
	useSnappy := flag.Bool("use_snappy", true, "")
	flag.Parse()

	err := rollingQueue(*dbPath, *totalN, *useSnappy)
	if err != nil {
		log.Fatalf("Benchmark errored unexpectedly: %v", err)
	}
}
