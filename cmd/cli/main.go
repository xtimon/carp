package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/carp/internal/client"
	"github.com/carp/internal/resp"
	"github.com/peterh/liner"
)

func main() {
	seeds := flag.String("h", "127.0.0.1:6379", "Seed address(es), comma-separated for multiple (e.g. 127.0.0.1:6379,127.0.0.1:6380)")
	consistency := flag.String("c", "", "Consistency level: ONE, QUORUM, or ALL (default: server QUORUM)")
	flag.Parse()

	seedList := strings.Split(*seeds, ",")
	for i, s := range seedList {
		seedList[i] = strings.TrimSpace(s)
		if seedList[i] == "" {
			seedList[i] = "127.0.0.1:6379"
		}
	}

	c := client.New(seedList)
	if *consistency != "" {
		cl := parseConsistency(*consistency)
		if cl != client.ConsistencyLevelDefault {
			c.SetConsistencyLevel(cl)
		}
	}

	args := flag.Args()
	if len(args) > 0 {
		// Non-interactive: execute command from args and exit
		cmd := args[0]
		var cmdArgs [][]byte
		for _, a := range args[1:] {
			cmdArgs = append(cmdArgs, []byte(a))
		}
		raw, err := c.Do(cmd, cmdArgs...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Print(resp.FormatResponse(raw))
		fmt.Println()
		return
	}

	// Interactive mode with completion and history
	line := liner.NewLiner()
	defer line.Close()
	line.SetCtrlCAborts(true)

	line.SetCompleter(func(lineStr string) (c []string) {
		// Liner replaces the entire pre-cursor text with the completion, so we must
		// return full-line completions (e.g. "CLUSTER KEYNODE" not just "KEYNODE").
		parts := strings.Fields(lineStr)
		var prefix string
		var head string // text before the word being completed
		if len(parts) == 0 {
			prefix = ""
			head = ""
		} else {
			prefix = strings.ToUpper(parts[len(parts)-1])
			// Reconstruct head (all parts except last, with spaces)
			if len(parts) > 1 {
				head = strings.Join(parts[:len(parts)-1], " ") + " "
			} else {
				head = ""
			}
		}

		if len(parts) <= 1 {
			for _, cmd := range redisCommands {
				if strings.HasPrefix(cmd, prefix) {
					c = append(c, cmd)
				}
			}
			return
		}
		first := strings.ToUpper(parts[0])
		if first == "CONSISTENCY" && len(parts) == 2 {
			for _, level := range []string{"ONE", "QUORUM", "ALL"} {
				if strings.HasPrefix(level, prefix) {
					c = append(c, head+level)
				}
			}
		}
		if first == "CLUSTER" && len(parts) == 2 {
			for _, sub := range []string{"INFO", "KEYNODE", "KEYSLOT", "NODES", "RING", "TOKEN"} {
				if strings.HasPrefix(sub, prefix) {
					c = append(c, head+sub)
				}
			}
		}
		return
	})

	home, _ := os.UserHomeDir()
	if home == "" {
		home = os.TempDir()
	}
	historyPath := filepath.Join(home, ".cass_redis_history")
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	prompt := seedList[0] + "> "
	for {
		input, err := line.Prompt(prompt)
		if err != nil {
			if err == liner.ErrPromptAborted {
				break
			}
			break
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		if input == "quit" || input == "exit" {
			break
		}

		line.AppendHistory(input)

		cmd, args := parseCommand(input)
		if len(cmd) == 0 {
			continue
		}

		// Handle CONSISTENCY as local session command
		if strings.ToUpper(cmd) == "CONSISTENCY" && len(args) >= 1 {
			cl := parseConsistency(string(args[0]))
			if cl != client.ConsistencyLevelDefault {
				c.SetConsistencyLevel(cl)
				fmt.Println("OK")
			} else {
				fmt.Printf("(error) invalid consistency level, use ONE, QUORUM, or ALL\n")
			}
			continue
		}

		raw, err := c.Do(cmd, args...)
		if err != nil {
			fmt.Printf("(error) %v\n", err)
			continue
		}
		out := resp.FormatResponse(raw)
		if out != "" {
			fmt.Println(out)
		}
	}

	if f, err := os.Create(historyPath); err == nil {
		line.WriteHistory(f)
		f.Close()
	}
}

var redisCommands = []string{
	"APPEND", "CONFIG", "CONSISTENCY", "DBSIZE", "DECR", "DECRBY", "DEL", "ECHO",
	"EXISTS", "EXPIRE", "FLUSHDB", "GET", "GETRANGE", "GETSET", "HELLO", "HEXISTS",
	"HGET", "HGETALL", "HKEYS", "HLEN", "HMGET", "HMSET", "HSET", "HVALS", "HDEL",
	"INCR", "INCRBY", "INFO", "KEYS", "LINDEX", "LLEN", "LPOP", "LPUSH", "LRANGE",
	"LREM", "LSET", "LTRIM", "MGET", "MSET", "PERSIST", "PING", "QUIT", "RANDOMKEY",
	"RPOP", "RPUSH", "SADD", "SCARD", "SET", "SETEX", "SETNX", "SETRANGE", "SISMEMBER",
	"SMEMBERS", "SPOP", "SREM", "STRLEN", "TIME", "TTL", "TYPE", "ZADD", "ZCARD",
	"ZRANGE", "ZRANK", "ZREM", "ZREVRANK", "ZSCORE",
	"CLUSTER",
}

func parseConsistency(s string) client.ConsistencyLevel {
	switch strings.ToUpper(s) {
	case "ONE":
		return client.ConsistencyOne
	case "QUORUM":
		return client.ConsistencyQuorum
	case "ALL":
		return client.ConsistencyAll
	}
	return client.ConsistencyLevelDefault
}

func parseCommand(line string) (string, [][]byte) {
	// Simple split: respect quoted strings
	var parts []string
	var buf bytes.Buffer
	inQuote := false
	quoteChar := byte(0)
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case inQuote:
			if c == quoteChar && (i+1 >= len(line) || (line[i+1] != quoteChar)) {
				inQuote = false
				parts = append(parts, buf.String())
				buf.Reset()
			} else if c == quoteChar && i+1 < len(line) && line[i+1] == quoteChar {
				buf.WriteByte(c)
				i++
			} else {
				buf.WriteByte(c)
			}
		case c == '"' || c == '\'':
			inQuote = true
			quoteChar = c
		case c == ' ' || c == '\t':
			if buf.Len() > 0 {
				parts = append(parts, buf.String())
				buf.Reset()
			}
		default:
			buf.WriteByte(c)
		}
	}
	if buf.Len() > 0 {
		parts = append(parts, buf.String())
	}
	if len(parts) == 0 {
		return "", nil
	}
	cmd := parts[0]
	var args [][]byte
	for _, p := range parts[1:] {
		args = append(args, []byte(p))
	}
	return cmd, args
}
