package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/carp/internal/cluster"
	"github.com/carp/internal/coordinator"
	"github.com/carp/internal/partitioner"
	"github.com/carp/internal/rebalance"
	"github.com/carp/internal/resp"
	"github.com/carp/internal/rpc"
	"github.com/carp/internal/storage"
)

type nodeConfig struct {
	NodeID            string `yaml:"node_id"`
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	GossipPort        int    `yaml:"gossip_port"`
	RPCPort           int    `yaml:"rpc_port"`
	Rack              string `yaml:"rack"` // logical grouping for fault tolerance (replicas spread across racks)
	ReplicationFactor int    `yaml:"replication_factor"`
	NumVnodes         int    `yaml:"vnodes"`
	ClusterName       string `yaml:"cluster_name"`
	Dir                   string `yaml:"dir"`                       // data directory for RDB (Redis-style)
	DBFilename            string `yaml:"dbfilename"`                // RDB filename (default: dump.rdb)
	SaveInterval          int    `yaml:"save_interval"`              // auto-save every N seconds; 0 = disabled
	TombstoneGraceSeconds int    `yaml:"tombstone_grace_seconds"`    // purge tombstones after N seconds; 0 = no GC
	SeedNodes         []struct {
		Host       string `yaml:"host"`
		GossipPort int    `yaml:"gossip_port"`
	} `yaml:"seed_nodes"`
}

// loadConfig loads config with precedence: defaults → file (if path non-empty) → env overrides.
func loadConfig(path string) (*nodeConfig, error) {
	cfg := &nodeConfig{
		NodeID:            getEnv("NODE_ID", "node1"),
		Host:              getEnv("HOST", "127.0.0.1"),
		Port:              getEnvInt("PORT", 6379),
		GossipPort:        getEnvInt("GOSSIP_PORT", 7000),
		RPCPort:           getEnvInt("RPC_PORT", 7379),
		Rack:              getEnv("RACK", ""),
		ReplicationFactor: getEnvInt("REPLICATION_FACTOR", 3),
		ClusterName:       getEnv("CLUSTER_NAME", "carp"),
		Dir:               getEnv("DIR", "./data"),
		DBFilename:        getEnv("DBFILENAME", "dump.rdb"),
		SaveInterval:          getEnvInt("SAVE_INTERVAL", 0),
		TombstoneGraceSeconds:  getEnvInt("TOMBSTONE_GRACE_SECONDS", 60),
	}
	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			expanded := expandEnv(string(data))
			var file nodeConfig
			if yaml.Unmarshal([]byte(expanded), &file) == nil {
				if file.NodeID != "" {
					cfg.NodeID = file.NodeID
				}
				if file.Host != "" {
					cfg.Host = file.Host
				}
				if file.Port != 0 {
					cfg.Port = file.Port
				}
				if file.GossipPort != 0 {
					cfg.GossipPort = file.GossipPort
				}
				if file.RPCPort != 0 {
					cfg.RPCPort = file.RPCPort
				}
				if file.Rack != "" {
					cfg.Rack = file.Rack
				}
				if file.ReplicationFactor != 0 {
					cfg.ReplicationFactor = file.ReplicationFactor
				}
				if file.NumVnodes > 0 {
					cfg.NumVnodes = file.NumVnodes
				}
				if file.ClusterName != "" {
					cfg.ClusterName = file.ClusterName
				}
				if len(file.SeedNodes) > 0 {
					cfg.SeedNodes = file.SeedNodes
				}
				if file.Dir != "" {
					cfg.Dir = file.Dir
				}
				if file.DBFilename != "" {
					cfg.DBFilename = file.DBFilename
				}
				if file.SaveInterval > 0 {
					cfg.SaveInterval = file.SaveInterval
				}
				if file.TombstoneGraceSeconds > 0 {
					cfg.TombstoneGraceSeconds = file.TombstoneGraceSeconds
				}
			}
		}
	}
	if v := getEnvInt("SAVE_INTERVAL", 0); v > 0 {
		cfg.SaveInterval = v
	}
	if v := getEnvInt("TOMBSTONE_GRACE_SECONDS", -1); v >= 0 {
		cfg.TombstoneGraceSeconds = v
	}
	// Env overrides
	if v := os.Getenv("CLUSTER_NAME"); v != "" {
		cfg.ClusterName = v
	}
	if v := os.Getenv("RACK"); v != "" {
		cfg.Rack = v
	}
	if v := getEnvInt("VNODES", 0); v > 0 {
		cfg.NumVnodes = v
	}
	if seeds := os.Getenv("SEEDS"); seeds != "" {
		cfg.SeedNodes = nil
		for _, s := range strings.Fields(seeds) {
			if idx := strings.LastIndex(s, ":"); idx >= 0 {
				host := s[:idx]
				port, _ := strconv.Atoi(s[idx+1:])
				cfg.SeedNodes = append(cfg.SeedNodes, struct {
					Host       string `yaml:"host"`
					GossipPort int    `yaml:"gossip_port"`
				}{Host: host, GossipPort: port})
			}
		}
	}
	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// expandEnv replaces ${VAR} and ${VAR:-default} in config strings with env values.
func expandEnv(s string) string {
	re := regexp.MustCompile(`\$\{([^}:]+)(?::-([^}]*))?\}`)
	return re.ReplaceAllStringFunc(s, func(match string) string {
		sub := re.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		key := sub[1]
		def := ""
		if len(sub) >= 3 {
			def = sub[2]
		}
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	})
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, _ := strconv.Atoi(v)
		return n
	}
	return def
}

func main() {
	configPath := flag.String("config", "", "Path to YAML config")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	store := storage.New()
	if cfg.TombstoneGraceSeconds > 0 {
		store.TombstoneGracePeriod = time.Duration(cfg.TombstoneGraceSeconds) * time.Second
	}
	rdbPath := filepath.Join(cfg.Dir, cfg.DBFilename)
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		log.Printf("[server] Warning: cannot create data dir %s: %v", cfg.Dir, err)
	} else if err := store.LoadFromFile(rdbPath); err != nil {
		log.Printf("[server] Warning: load RDB %s: %v", rdbPath, err)
	} else {
		log.Printf("[server] Loaded RDB from %s", rdbPath)
	}
	gossip := cluster.NewGossip(cfg.NodeID, cfg.Host, cfg.Port, cfg.GossipPort, cfg.ClusterName, cfg.Rack)
	part := partitioner.NewPartitioner(cfg.ReplicationFactor)
	if cfg.NumVnodes > 0 {
		part.SetNumVnodes(cfg.NumVnodes)
	}
	coord := coordinator.New(cfg.NodeID, gossip, part, store, cfg.ReplicationFactor, cfg.ClusterName)
	shutdownCh := make(chan struct{}, 1)
	coord.SetShutdownCh(shutdownCh)
	coord.SetSavePath(rdbPath)

	for _, s := range cfg.SeedNodes {
		gossip.AddSeed(s.Host, s.GossipPort)
	}
	if len(cfg.SeedNodes) > 0 {
		seedAddrs := make([]string, 0, len(cfg.SeedNodes))
		for _, s := range cfg.SeedNodes {
			seedAddrs = append(seedAddrs, fmt.Sprintf("%s:%d", s.Host, s.GossipPort))
		}
		log.Printf("[server] Gossip seeds: %v (gossip_port=%d)", seedAddrs, cfg.GossipPort)
	} else {
		log.Printf("[server] No seeds - node will not discover peers (fine for single-node)")
	}

	// Gossip owns partitioner refresh: updates only when ring nodes stable for 2 rounds
	if cfg.NumVnodes > 0 {
		gossip.SetNumVnodes(cfg.NumVnodes)
	}
	gossip.SetOnRingChange(func(nodeIDs []string) {
		if len(nodeIDs) > 0 {
			rackMap := make(map[string]string)
			for _, n := range gossip.GetRingNodeStates() {
				rackMap[n.NodeID] = n.Rack
			}
			part.SetNodes(nodeIDs, rackMap)
		}
	})

	// Initial partitioner from self (gossip will update when ring stabilizes)
	rackMap := map[string]string{cfg.NodeID: cfg.Rack}
	part.SetNodes([]string{cfg.NodeID}, rackMap)

	gossip.Start()
	gsrv := cluster.NewGossipServer(gossip)
	if err := gsrv.Start(); err != nil {
		log.Fatal(err)
	}
	defer gsrv.Close()

	// Rebalancer (needed before RPC for repair callback)
	rb := rebalance.New(cfg.NodeID, gossip, part, store)
	rb.Start()
	coord.SetRepairFunc(rb.RunRepair)
	coord.SetRepairFuncSmooth(rb.RunRepairSmooth)

	// RPC server
	rpcLn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RPCPort))
	if err != nil {
		log.Fatal(err)
	}
	defer rpcLn.Close()
	log.Printf("[server] RPC server on %s:%d", cfg.Host, cfg.RPCPort)
	rpcHandler := &rpc.Handler{
		Store:          store,
		OnRepair:       rb.RunRepair,
		OnRepairSmooth: rb.RunRepairSmooth,
	}
	go func() {
		for {
			conn, err := rpcLn.Accept()
			if err != nil {
				return
			}
			go rpc.HandleConn(conn, rpcHandler)
		}
	}()

	// Redis (RESP) server
	respLn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
	if err != nil {
		log.Fatal(err)
	}
	defer respLn.Close()
	log.Printf("[server] Redis (RESP) server on %s:%d", cfg.Host, cfg.Port)
	log.Printf("[server] Node %s ready. Connect with: redis-cli -p %d", cfg.NodeID, cfg.Port)

	// Auto-save (Redis-style: periodic BGSAVE)
	if cfg.SaveInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(cfg.SaveInterval) * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				if err := store.SaveToFile(rdbPath); err != nil {
					log.Printf("[server] Auto-save failed: %v", err)
				}
			}
		}()
		log.Printf("[server] Auto-save every %ds to %s", cfg.SaveInterval, rdbPath)
	}

	// Tombstone GC: purge tombstones older than grace period
	if cfg.TombstoneGraceSeconds > 0 {
		interval := time.Duration(cfg.TombstoneGraceSeconds/2) * time.Second
		if interval < 10*time.Second {
			interval = 10 * time.Second
		}
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				if n := store.RunTombstoneGC(); n > 0 {
					log.Printf("[server] Tombstone GC purged %d entries", n)
				}
			}
		}()
		log.Printf("[server] Tombstone grace period %ds (GC every %v)", cfg.TombstoneGraceSeconds, interval)
	}

	// Shutdown on CLUSTER LEAVE
	go func() {
		<-shutdownCh
		log.Printf("[server] Shutting down node %s", cfg.NodeID)
		// Save before exit (Redis SHUTDOWN SAVE style)
		if err := store.SaveToFile(rdbPath); err != nil {
			log.Printf("[server] Save on shutdown failed: %v", err)
		}
		gossip.Stop()
		rb.Stop()
		gsrv.Close()
		rpcLn.Close()
		respLn.Close()
		os.Exit(0)
	}()

	for {
		conn, err := respLn.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go handleRESP(conn, coord)
	}
}

func handleRESP(conn net.Conn, coord *coordinator.Coordinator) {
	defer conn.Close()
	reader := resp.Reader{}
	buf := make([]byte, 65536)
	var connConsistency *coordinator.ConsistencyLevel
	for {
		n, err := conn.Read(buf)
		if err != nil || n == 0 {
			return
		}
		reader.Feed(buf[:n])
		for _, cmd := range reader.ParseCommands() {
			if len(cmd) == 0 {
				continue
			}
			cmdName := cmd[0]
			args := cmd[1:]
			cmdStr := strings.ToUpper(string(cmdName))
			if cmdStr == "CONSISTENCY" {
				if len(args) < 1 {
					conn.Write(resp.EncodeError("wrong number of arguments for 'consistency' command"))
					continue
				}
				if cl, ok := coordinator.ParseConsistencyLevel(args[0]); ok {
					connConsistency = &cl
					conn.Write(resp.EncodeSimpleString("OK"))
				} else {
					conn.Write(resp.EncodeError("invalid consistency level, use ONE, QUORUM, or ALL"))
				}
				continue
			}
			response := coord.Execute(cmdName, args, connConsistency)
			conn.Write(response)
		}
	}
}
