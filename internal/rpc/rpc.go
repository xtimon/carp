package rpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/carp/internal/storage"
)

const rpcPoolSize = 8 // max pooled connections per addr to limit port usage

var (
	poolMu  sync.Mutex
	connPools = make(map[string]*rpcConnPool)
)

type rpcConnPool struct {
	addr  string
	conns chan net.Conn
}

// getPool returns or creates a connection pool per address (limits port exhaustion from many concurrent RPCs).
func getPool(addr string) *rpcConnPool {
	poolMu.Lock()
	defer poolMu.Unlock()
	if p := connPools[addr]; p != nil {
		return p
	}
	p := &rpcConnPool{
		addr:  addr,
		conns: make(chan net.Conn, rpcPoolSize),
	}
	connPools[addr] = p
	return p
}

func (p *rpcConnPool) get() (net.Conn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		return net.DialTimeout("tcp", p.addr, 5*time.Second)
	}
}

func (p *rpcConnPool) put(conn net.Conn) {
	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

// Command codes
const (
	CmdGet     = 1
	CmdSet     = 2
	CmdDel     = 3
	CmdExists  = 4
	CmdKeys    = 5
	CmdIncr    = 6
	CmdTTL     = 7
	CmdExpire  = 8
	CmdPersist = 9
	CmdLPush   = 10
	CmdRPush   = 11
	CmdLLen    = 12
	CmdLRange  = 13
	CmdLPop    = 14
)

// rpcPortOffset: RPC listens on Redis port + 1000 (e.g. Redis 6379 → RPC 7379).
// Config RPCPort overrides this when set explicitly.
const rpcPortOffset = 1000

// SendCommand sends internal command to replica (uses connection pool to reduce port exhaustion)
func SendCommand(host string, redisPort int, cmd byte, args [][]byte) ([]byte, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(redisPort+rpcPortOffset))
	pool := getPool(addr)
	conn, err := pool.get()
	if err != nil {
		return nil, err
	}
	defer func() {
		if conn != nil {
			pool.put(conn)
		}
	}()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	msg := packCommand(cmd, args)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msg)))
	if _, err := conn.Write(lenBuf); err != nil {
		conn.Close()
		conn = nil
		return nil, err
	}
	if _, err := conn.Write(msg); err != nil {
		conn.Close()
		conn = nil
		return nil, err
	}
	if _, err := conn.Read(lenBuf); err != nil {
		conn.Close()
		conn = nil
		return nil, err
	}
	respLen := binary.BigEndian.Uint32(lenBuf)
	if respLen == 0 {
		return nil, nil
	}
	resp := make([]byte, respLen)
	if _, err := conn.Read(resp); err != nil {
		conn.Close()
		conn = nil
		return nil, err
	}
	return resp, nil
}

// packCommand formats: [1 byte cmd][2 byte arg count][foreach arg: 4 byte len, bytes]
func packCommand(cmd byte, args [][]byte) []byte {
	size := 1 + 2
	for _, a := range args {
		size += 4 + len(a)
	}
	buf := make([]byte, size)
	buf[0] = cmd
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(args)))
	off := 3
	for _, a := range args {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(a)))
		off += 4
		copy(buf[off:], a)
		off += len(a)
	}
	return buf
}

func unpackCommand(data []byte) (byte, [][]byte, error) {
	if len(data) < 3 {
		return 0, nil, errors.New("truncated")
	}
	cmd := data[0]
	n := binary.BigEndian.Uint16(data[1:3])
	var args [][]byte
	off := 3
	for i := 0; i < int(n) && off+4 <= len(data); i++ {
		alen := binary.BigEndian.Uint32(data[off:])
		off += 4
		args = append(args, data[off:off+int(alen)])
		off += int(alen)
	}
	return cmd, args, nil
}

// Handler holds RPC handler configuration
type Handler struct {
	Store         *storage.Storage
	OnRepair      func() int           // called for CmdRunRepair; nil = no-op
	OnRepairSmooth func(time.Duration) int // called for CmdRunRepairSmooth; nil = no-op
}

// HandleConn processes RPC connection; dispatches by command code (see rpc_ext.go for extended codes).
func HandleConn(conn net.Conn, h *Handler) {
	store := h.Store
	defer conn.Close()
	lenBuf := make([]byte, 4)
	if _, err := conn.Read(lenBuf); err != nil {
		return
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)
	msg := make([]byte, msgLen)
	if _, err := conn.Read(msg); err != nil {
		return
	}
	cmd, args, err := unpackCommand(msg)
	if err != nil {
		return
	}
	var result []byte
	switch cmd {
	case CmdGet:
		if len(args) >= 1 {
			val, _ := store.Get(args[0])
			result = val
		}
	case CmdSet:
		if len(args) >= 2 {
			store.Set(args[0], args[1], nil)
			result = []byte("OK")
		}
	case CmdDel:
		if len(args) >= 1 {
			ok, _ := store.Delete(args[0])
			if ok {
				result = []byte("1")
			} else {
				result = []byte("0")
			}
		}
	case CmdExists:
		if len(args) >= 1 {
			n, _ := store.Exists(args[0])
			result = []byte{byte(n)}
		}
	case CmdKeys:
		if len(args) >= 1 {
			keys, _ := store.Keys(args[0])
			for i, k := range keys {
				if i > 0 {
					result = append(result, '\n')
				}
				result = append(result, k...)
			}
		}
	case CmdIncr:
		if len(args) >= 1 {
			val, err := store.Incr(args[0], 1)
			if err != nil {
				result = []byte("ERR")
			} else {
				result = []byte(strconv.Itoa(val))
			}
		}
	case CmdTTL:
		if len(args) >= 1 {
			t, _ := store.TTL(args[0])
			result = []byte(strconv.Itoa(t))
		}
	case CmdExpire:
		if len(args) >= 2 {
			secs, _ := strconv.Atoi(string(args[1]))
			ok, _ := store.Expire(args[0], secs)
			if ok {
				result = []byte("1")
			} else {
				result = []byte("0")
			}
		}
	case CmdPersist:
		if len(args) >= 1 {
			ok, _ := store.Persist(args[0])
			if ok {
				result = []byte("1")
			} else {
				result = []byte("0")
			}
		}
	case CmdLPush:
		if len(args) >= 2 {
			n, _ := store.LPush(args[0], args[1:]...)
			result = []byte(strconv.Itoa(n))
		}
	case CmdRPush:
		if len(args) >= 2 {
			n, _ := store.RPush(args[0], args[1:]...)
			result = []byte(strconv.Itoa(n))
		}
	case CmdLLen:
		if len(args) >= 1 {
			n, _ := store.LLen(args[0])
			result = []byte(strconv.Itoa(n))
		}
	case CmdLRange:
		if len(args) >= 3 {
			start, _ := strconv.Atoi(string(args[1]))
			stop, _ := strconv.Atoi(string(args[2]))
			items, _ := store.LRange(args[0], start, stop)
			// Length-prefixed format: [4-byte n][for each: 4-byte len + data]
			size := 4
			for _, it := range items {
				size += 4 + len(it)
			}
			buf := make([]byte, size)
			binary.BigEndian.PutUint32(buf[0:4], uint32(len(items)))
			off := 4
			for _, it := range items {
				binary.BigEndian.PutUint32(buf[off:], uint32(len(it)))
				off += 4
				copy(buf[off:], it)
				off += len(it)
			}
			result = buf
		}
	case CmdLPop:
		if len(args) >= 1 {
			val, _ := store.LPop(args[0])
			result = val
		}
	// Strings
	case CmdStrlen:
		if len(args) >= 1 {
			n, _ := store.Strlen(args[0])
			result = []byte(strconv.Itoa(n))
		}
	case CmdAppend:
		if len(args) >= 2 {
			n, _ := store.Append(args[0], args[1])
			result = []byte(strconv.Itoa(n))
		}
	case CmdGetRange:
		if len(args) >= 3 {
			start, _ := strconv.Atoi(string(args[1]))
			end, _ := strconv.Atoi(string(args[2]))
			val, _ := store.GetRange(args[0], start, end)
			result = val
		}
	case CmdSetRange:
		if len(args) >= 3 {
			offset, _ := strconv.Atoi(string(args[1]))
			n, _ := store.SetRange(args[0], offset, args[2])
			result = []byte(strconv.Itoa(n))
		}
	case CmdIncrBy:
		if len(args) >= 2 {
			delta, _ := strconv.Atoi(string(args[1]))
			val, err := store.IncrBy(args[0], delta)
			if err != nil {
				result = []byte("ERR")
			} else {
				result = []byte(strconv.Itoa(val))
			}
		}
	case CmdSetNX:
		if len(args) >= 2 {
			n, _ := store.SetNX(args[0], args[1])
			result = []byte(strconv.Itoa(n))
		}
	case CmdGetSet:
		if len(args) >= 2 {
			old, _ := store.GetSet(args[0], args[1])
			result = old
		}
	case CmdIncrByIdem:
		if len(args) >= 2 {
			delta, _ := strconv.Atoi(string(args[1]))
			token := []byte(nil)
			if len(args) >= 3 {
				token = args[2]
			}
			val, err := store.IncrByWithIdempotency(args[0], token, delta)
			if err != nil {
				result = []byte("ERR")
			} else {
				result = []byte(strconv.Itoa(val))
			}
		}
	case CmdIncrByRepl:
		if len(args) >= 3 {
			resultVal, _ := strconv.Atoi(string(args[1]))
			store.Set(args[0], []byte(strconv.Itoa(resultVal)), nil)
			store.IdempotencyPut(args[0], args[2], resultVal)
			result = []byte(strconv.Itoa(resultVal))
		}
	// Lists
	case CmdRPop:
		if len(args) >= 1 {
			val, _ := store.RPop(args[0])
			result = val
		}
	case CmdLIndex:
		if len(args) >= 2 {
			idx, _ := strconv.Atoi(string(args[1]))
			val, _ := store.LIndex(args[0], idx)
			result = val
		}
	case CmdLSet:
		if len(args) >= 3 {
			idx, _ := strconv.Atoi(string(args[1]))
			store.LSet(args[0], idx, args[2])
			result = []byte("OK")
		}
	case CmdLRem:
		if len(args) >= 3 {
			count, _ := strconv.Atoi(string(args[1]))
			n, _ := store.LRem(args[0], count, args[2])
			result = []byte(strconv.Itoa(n))
		}
	case CmdLTrim:
		if len(args) >= 3 {
			start, _ := strconv.Atoi(string(args[1]))
			stop, _ := strconv.Atoi(string(args[2]))
			store.LTrim(args[0], start, stop)
			result = []byte("OK")
		}
	// Sets
	case CmdSAdd:
		if len(args) >= 2 {
			n, _ := store.SAdd(args[0], args[1:]...)
			result = []byte(strconv.Itoa(n))
		}
	case CmdSRem:
		if len(args) >= 2 {
			n, _ := store.SRem(args[0], args[1:]...)
			result = []byte(strconv.Itoa(n))
		}
	case CmdSIsMember:
		if len(args) >= 2 {
			n, _ := store.SIsMember(args[0], args[1])
			result = []byte(strconv.Itoa(n))
		}
	case CmdSMembers:
		if len(args) >= 1 {
			members, _ := store.SMembers(args[0])
			for i, m := range members {
				if i > 0 {
					result = append(result, '\n')
				}
				result = append(result, m...)
			}
		}
	case CmdSCard:
		if len(args) >= 1 {
			n, _ := store.SCard(args[0])
			result = []byte(strconv.Itoa(n))
		}
	case CmdSPop:
		if len(args) >= 1 {
			val, _ := store.SPop(args[0])
			result = val
		}
	// Hashes
	case CmdHSet:
		if len(args) >= 3 {
			n, _ := store.HSet(args[0], args[1], args[2])
			result = []byte(strconv.Itoa(n))
		}
	case CmdHGet:
		if len(args) >= 2 {
			val, _ := store.HGet(args[0], args[1])
			result = val
		}
	case CmdHDel:
		if len(args) >= 2 {
			n, _ := store.HDel(args[0], args[1:]...)
			result = []byte(strconv.Itoa(n))
		}
	case CmdHExists:
		if len(args) >= 2 {
			n, _ := store.HExists(args[0], args[1])
			result = []byte(strconv.Itoa(n))
		}
	case CmdHLen:
		if len(args) >= 1 {
			n, _ := store.HLen(args[0])
			result = []byte(strconv.Itoa(n))
		}
	case CmdHGetAll:
		if len(args) >= 1 {
			pairs, _ := store.HGetAll(args[0])
			size := 4
			for _, p := range pairs {
				size += 4 + len(p)
			}
			buf := make([]byte, size)
			binary.BigEndian.PutUint32(buf[0:4], uint32(len(pairs)))
			off := 4
			for _, p := range pairs {
				binary.BigEndian.PutUint32(buf[off:], uint32(len(p)))
				off += 4
				copy(buf[off:], p)
				off += len(p)
			}
			result = buf
		}
	case CmdHKeys:
		if len(args) >= 1 {
			keys, _ := store.HKeys(args[0])
			for i, k := range keys {
				if i > 0 {
					result = append(result, '\n')
				}
				result = append(result, k...)
			}
		}
	case CmdHVals:
		if len(args) >= 1 {
			vals, _ := store.HVals(args[0])
			for i, v := range vals {
				if i > 0 {
					result = append(result, '\n')
				}
				result = append(result, v...)
			}
		}
	case CmdHMSet:
		if len(args) >= 3 {
			for i := 1; i+1 < len(args); i += 2 {
				store.HSet(args[0], args[i], args[i+1])
			}
			result = []byte("OK")
		}
	case CmdHMGet:
		if len(args) >= 2 {
			size := 4
			for i := 1; i < len(args); i++ {
				val, _ := store.HGet(args[0], args[i])
				size += 4 + len(val)
			}
			buf := make([]byte, size)
			binary.BigEndian.PutUint32(buf[0:4], uint32(len(args)-1))
			off := 4
			for i := 1; i < len(args); i++ {
				val, _ := store.HGet(args[0], args[i])
				binary.BigEndian.PutUint32(buf[off:], uint32(len(val)))
				off += 4
				copy(buf[off:], val)
				off += len(val)
			}
			result = buf
		}
	// Sorted Sets
	case CmdZAdd:
		if len(args) >= 3 {
			n, _ := store.ZAdd(args[0], args[1:])
			result = []byte(strconv.Itoa(n))
		}
	case CmdZRem:
		if len(args) >= 2 {
			n, _ := store.ZRem(args[0], args[1:]...)
			result = []byte(strconv.Itoa(n))
		}
	case CmdZScore:
		if len(args) >= 2 {
			score, ok, _ := store.ZScore(args[0], args[1])
			if ok {
				result = []byte(strconv.FormatFloat(score, 'f', -1, 64))
			}
		}
	case CmdZCard:
		if len(args) >= 1 {
			n, _ := store.ZCard(args[0])
			result = []byte(strconv.Itoa(n))
		}
	case CmdZRank:
		if len(args) >= 2 {
			rank, _ := store.ZRank(args[0], args[1])
			result = []byte(strconv.Itoa(rank))
		}
	case CmdZRevRank:
		if len(args) >= 2 {
			rank, _ := store.ZRevRank(args[0], args[1])
			result = []byte(strconv.Itoa(rank))
		}
	case CmdZRange:
		if len(args) >= 3 {
			start, _ := strconv.Atoi(string(args[1]))
			stop, _ := strconv.Atoi(string(args[2]))
			withScores := len(args) >= 4 && bytes.EqualFold(args[3], []byte("WITHSCORES"))
			items, _ := store.ZRange(args[0], start, stop, withScores)
			size := 4
			for _, it := range items {
				size += 4 + len(it)
			}
			buf := make([]byte, size)
			binary.BigEndian.PutUint32(buf[0:4], uint32(len(items)))
			off := 4
			for _, it := range items {
				binary.BigEndian.PutUint32(buf[off:], uint32(len(it)))
				off += 4
				copy(buf[off:], it)
				off += len(it)
			}
			result = buf
		}
	// Server/Key
	case CmdType:
		if len(args) >= 1 {
			result = []byte(store.Type(args[0]))
		}
	case CmdDBSize:
		n, _ := store.DBSize()
		result = []byte(strconv.Itoa(n))
	case CmdFlushDB:
		store.FlushDB()
		result = []byte("OK")
	case CmdRandomKey:
		val, _ := store.RandomKey()
		result = val
	case CmdDumpKey:
		if len(args) >= 1 {
			dump, _ := store.DumpKey(args[0])
			result = dump
		}
	case CmdRestoreKey:
		if len(args) >= 2 {
			err := store.RestoreKey(args[0], args[1])
			if err != nil {
				result = []byte("ERR:" + err.Error())
			} else {
				result = []byte("OK")
			}
		}
	case CmdSetTombstone:
		if len(args) >= 1 {
			ok, _ := store.SetTombstone(args[0])
			if ok {
				result = []byte("1")
			} else {
				result = []byte("0")
			}
		}
	case CmdRunRepair:
		if h.OnRepair != nil {
			n := h.OnRepair()
			result = []byte(strconv.Itoa(n))
		} else {
			result = []byte("0")
		}
	case CmdRunRepairSmooth:
		delayMs := 100
		if len(args) >= 1 {
			if d, err := strconv.Atoi(string(args[0])); err == nil && d > 0 {
				delayMs = d
			}
		}
		if h.OnRepairSmooth != nil {
			n := h.OnRepairSmooth(time.Duration(delayMs) * time.Millisecond)
			result = []byte(strconv.Itoa(n))
		} else {
			result = []byte("0")
		}
	case CmdRunTombstoneGC:
		n := store.RunTombstoneGC()
		result = []byte(strconv.Itoa(n))
	}
	if result == nil {
		result = []byte{}
	}
	binary.BigEndian.PutUint32(lenBuf, uint32(len(result)))
	conn.Write(lenBuf)
	conn.Write(result)
}
