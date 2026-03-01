package rpc

// Extended command codes (15+)
const (
	CmdStrlen   = 20
	CmdAppend   = 21
	CmdGetRange = 22
	CmdSetRange = 23
	CmdIncrBy   = 24
	CmdSetNX    = 25
	CmdSetEX    = 26
	CmdGetSet       = 27
	CmdIncrByIdem   = 28 // IncrBy with idempotency token (Netflix: safe retry)
	CmdIncrByRepl   = 29 // Replicate incr result + idempotency (args: key, result, token)
	CmdRPop     = 30
	CmdLIndex   = 31
	CmdLSet     = 32
	CmdLRem     = 33
	CmdLTrim    = 34
	CmdSAdd     = 40
	CmdSRem     = 41
	CmdSIsMember = 42
	CmdSMembers = 43
	CmdSCard    = 44
	CmdSPop     = 45
	CmdHSet     = 50
	CmdHGet     = 51
	CmdHDel     = 52
	CmdHExists  = 53
	CmdHLen     = 54
	CmdHGetAll  = 55
	CmdHKeys    = 56
	CmdHVals    = 57
	CmdHMSet    = 58
	CmdHMGet    = 59
	CmdZAdd     = 60
	CmdZRem     = 61
	CmdZScore   = 62
	CmdZCard    = 63
	CmdZRank    = 64
	CmdZRevRank = 65
	CmdZRange   = 66
	CmdType     = 70
	CmdDBSize   = 71
	CmdFlushDB  = 72
	CmdRandomKey = 73
	CmdDumpKey      = 80 // DumpKey(key) -> serialized payload for migration
	CmdRestoreKey   = 81 // RestoreKey(key, payload) -> restore from migration
	CmdSetTombstone   = 84 // SetTombstone(key) -> mark deleted for replication
	CmdRunRepair      = 82 // RunRepair() -> trigger local repair (no args)
	CmdRunRepairSmooth = 83 // RunRepairSmooth(delay_ms) -> smooth repair vnode by vnode
	CmdRunTombstoneGC = 85 // RunTombstoneGC() -> purge expired tombstones, returns count
)
