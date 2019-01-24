package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
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

func bulkInsertAndSearch(
	dbPath string, totalN int, useSnappy bool, useBulk bool) error {
	dbPath = path.Join(dbPath, "bench_bulkinsertandsearch.wt")
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

	if err := s.Drop("table:test1", &wt.DropConfig{Force: wt.True}); err != nil {
		return err
	}
	tableCfg := &wt.DataSourceConfig{}
	if useSnappy {
		tableCfg.BlockCompressor = "snappy"
	}
	if err := s.Create("table:test1", tableCfg); err != nil {
		return err
	}

	m, err := s.Mutate("table:test1", &wt.MutationConfig{Bulk: &useBulk})
	if err != nil {
		return err
	}

	rr := rand.New(rand.NewSource(77))
	t0 := time.Now()
	for i := 0; i < totalN; i++ {
		key := makeKey(i)
		value := makeValue(rr)
		if err := m.Insert(key, value); err != nil {
			return err
		}

		if i%(totalN/10) == 0 {
			tDelta := time.Now().Sub(t0)
			log.Printf("elapsed: %v, per item: %v", tDelta, tDelta/time.Duration(i+1))
		}
	}
	tDelta := time.Now().Sub(t0)
	log.Printf("Insert took: %v, per item: %v", tDelta, tDelta/time.Duration(totalN))
	if err := m.Close(); err != nil {
		return err
	}

	t0 = time.Now()
	scanner, err := s.Scan("table:test1")
	if err != nil {
		return err
	}
	log.Print("Scanning Keys......")
	for i := 0; ; i++ {
		if err := scanner.Next(); err != nil {
			log.Printf("Scan Finished: %v, items: %v", err, i)
			break
		}

		key, err := scanner.UnsafeKey()
		if err != nil {
			return err
		}
		if i%(totalN/10) == 0 {
			log.Printf("%s", key)
		}
	}
	log.Printf("Scan took: %v", time.Now().Sub(t0))

	if err := scanner.Reset(); err != nil {
		return err
	}

	searchN := 20
	t0 = time.Now()
	for i := 0; i < searchN; i++ {
		keyIdx := rr.Intn(totalN)
		if err := scanner.Search(makeKey(keyIdx)); err != nil {
			return err
		}
		key, err := scanner.UnsafeKey()
		if err != nil {
			return err
		}
		value, err := scanner.UnsafeValue()
		if err != nil {
			return err
		}
		log.Printf("Search Key: %s:%s", key, hex.EncodeToString(value)[:40])
	}
	tDelta = time.Now().Sub(t0)
	log.Printf("Searches Took: %v, Per Search: %v", tDelta, tDelta/time.Duration(searchN))

	if err := scanner.Close(); err != nil {
		return err
	}

	return nil
}

func main() {
	dbPath := flag.String("path", "/tmp", "")
	totalN := flag.Int("total_items", 10000, "")
	useSnappy := flag.Bool("use_snappy", true, "")
	useBulk := flag.Bool("use_bulk", false, "")
	flag.Parse()

	err := bulkInsertAndSearch(*dbPath, *totalN, *useSnappy, *useBulk)
	if err != nil {
		log.Fatalf("Benchmark errored unexpectedly: %v", err)
	}
}
