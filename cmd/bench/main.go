package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/carp/internal/client"
)

func main() {
	seeds := flag.String("h", "127.0.0.1:6379", "Seed address(es), comma-separated (e.g. 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381)")
	clients := flag.Int("c", 10, "Number of parallel connections (clients)")
	requests := flag.Int("n", 100000, "Total number of requests")
	datasize := flag.Int("d", 3, "Data size of SET/GET value in bytes")
	keyspace := flag.Int("r", 100000, "Use random keys from a keyspace of size N")
	tests := flag.String("t", "ping,set,get,incr,lpush,lpop,sadd,spop,hset,hget,zadd,zrange", "Comma-separated list of tests to run")
	consistency := flag.String("C", "", "Consistency level: ONE, QUORUM, or ALL")
	quiet := flag.Bool("q", false, "Quiet mode - only show summary")
	duration := flag.Duration("D", 0, "Run for specified duration (overrides -n)")
	flag.Parse()

	seedList := parseSeeds(*seeds)
	cl := parseConsistency(*consistency)
	testList := strings.Split(strings.ToLower(*tests), ",")
	for i, t := range testList {
		testList[i] = strings.TrimSpace(t)
	}

	fmt.Printf("carp-bench - Ring-aware benchmark for CARP\n")
	fmt.Printf("Seeds: %v | Clients: %d | Requests: %d | Data size: %d | Keyspace: %d\n\n",
		seedList, *clients, *requests, *datasize, *keyspace)

	// Pre-generate value for SET
	value := make([]byte, *datasize)
	for i := range value {
		value[i] = 'x'
	}

	rand.Seed(time.Now().UnixNano())
	totalDuration := *duration
	if totalDuration == 0 {
		totalDuration = time.Hour // will stop by request count
	}

	for _, t := range testList {
		switch t {
		case "ping":
			runTest("PING", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, _ int) error {
				_, err := c.Do("PING")
				return err
			})
		case "set":
			runTest("SET", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:key:%d", rand.Intn(*keyspace)))
				_, err := c.Do("SET", key, value)
				return err
			})
		case "get":
			runTest("GET", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:key:%d", rand.Intn(*keyspace)))
				_, err := c.Do("GET", key)
				return err
			})
		case "incr":
			runTest("INCR", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:incr:%d", rand.Intn(*keyspace)))
				_, err := c.Do("INCR", key)
				return err
			})
		case "lpush":
			runTest("LPUSH", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:list:%d", rand.Intn(*keyspace)))
				_, err := c.Do("LPUSH", key, value)
				return err
			})
		case "lpop":
			runTest("LPOP", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:list:%d", rand.Intn(*keyspace)))
				_, err := c.Do("LPOP", key)
				return err
			})
		case "sadd":
			runTest("SADD", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:set:%d", rand.Intn(*keyspace)))
				member := []byte(fmt.Sprintf("member:%d", i))
				_, err := c.Do("SADD", key, member)
				return err
			})
		case "spop":
			runTest("SPOP", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:set:%d", rand.Intn(*keyspace)))
				_, err := c.Do("SPOP", key)
				return err
			})
		case "hset":
			runTest("HSET", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:hash:%d", rand.Intn(*keyspace)))
				field := []byte(fmt.Sprintf("f%d", i%100))
				_, err := c.Do("HSET", key, field, value)
				return err
			})
		case "hget":
			runTest("HGET", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:hash:%d", rand.Intn(*keyspace)))
				field := []byte(fmt.Sprintf("f%d", i%100))
				_, err := c.Do("HGET", key, field)
				return err
			})
		case "zadd":
			runTest("ZADD", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:zset:%d", rand.Intn(*keyspace)))
				member := []byte(fmt.Sprintf("m%d", i))
				score := []byte(fmt.Sprintf("%d", i))
				_, err := c.Do("ZADD", key, score, member)
				return err
			})
		case "zrange":
			runTest("ZRANGE", *clients, *requests, totalDuration, *quiet, seedList, cl, func(c *client.Client, i int) error {
				key := []byte(fmt.Sprintf("bench:zset:%d", rand.Intn(*keyspace)))
				_, err := c.Do("ZRANGE", key, []byte("0"), []byte("9"))
				return err
			})
		case "":
			// skip empty
		default:
			fmt.Printf("Unknown test: %s (skipping)\n", t)
		}
	}
}

func parseSeeds(s string) []string {
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"127.0.0.1:6379"}
	}
	return out
}

func parseConsistency(s string) client.ConsistencyLevel {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "ONE":
		return client.ConsistencyOne
	case "QUORUM":
		return client.ConsistencyQuorum
	case "ALL":
		return client.ConsistencyAll
	}
	return client.ConsistencyLevelDefault
}

type benchFn func(*client.Client, int) error

func runTest(name string, numClients, numRequests int, maxDuration time.Duration, quiet bool, seeds []string, consistency client.ConsistencyLevel, fn benchFn) {
	reqPerClient := (numRequests + numClients - 1) / numClients

	var completed atomic.Int64
	var errors atomic.Int64
	var latenciesMu sync.Mutex
	latencies := make([]float64, 0, numRequests)

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := client.New(seeds)
			if consistency != client.ConsistencyLevelDefault {
				c.SetConsistencyLevel(consistency)
			}
			for j := 0; j < reqPerClient; j++ {
				if time.Since(start) >= maxDuration {
					break
				}
				t0 := time.Now()
				err := fn(c, j)
				elapsed := time.Since(t0).Seconds() * 1000
				completed.Add(1)
				if err != nil {
					errors.Add(1)
				}
				latenciesMu.Lock()
				latencies = append(latencies, elapsed)
				latenciesMu.Unlock()
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()
	total := completed.Load()
	errs := errors.Load()

	if total == 0 {
		fmt.Printf("%s: no requests completed\n", name)
		return
	}

	qps := float64(total) / elapsed
	if quiet {
		fmt.Printf("%s: %.0f requests per second\n", name, qps)
		return
	}

	// Sort for percentiles
	latenciesMu.Lock()
	sort.Float64s(latencies)
	p50, p95, p99, pMax := 0.0, 0.0, 0.0, 0.0
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
		pMax = latencies[len(latencies)-1]
	}
	latenciesMu.Unlock()

	fmt.Printf("====== %s ======\n", name)
	fmt.Printf("  %d requests completed in %.2f seconds\n", total, elapsed)
	if errs > 0 {
		fmt.Printf("  %d errors\n", errs)
	}
	fmt.Printf("  %.2f requests per second\n", qps)
	fmt.Printf("  Latency: p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms\n", p50, p95, p99, pMax)
	fmt.Println()
}
