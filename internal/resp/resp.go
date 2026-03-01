package resp

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var crlf = []byte("\r\n")

// EncodeCommand encodes a Redis command as a RESP array for sending over the wire.
// Writes directly to a single buffer to avoid intermediate allocations.
func EncodeCommand(cmd string, args ...[]byte) []byte {
	n := 1 + len(args)
	size := 4 + len(crlf) // "*N\r\n"
	size += 5 + len(cmd) + len(crlf)
	for _, a := range args {
		if a == nil {
			size += 5 + len(crlf)
		} else {
			size += 5 + len(a) + len(crlf)
		}
	}
	buf := make([]byte, 0, size)
	buf = append(buf, '*')
	buf = append(buf, strconv.Itoa(n)...)
	buf = append(buf, crlf...)
	buf = appendBulkStringStr(buf, cmd)
	for _, a := range args {
		buf = appendBulkString(buf, a)
	}
	return buf
}

func appendBulkStringStr(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.Itoa(len(s))...)
	buf = append(buf, crlf...)
	buf = append(buf, s...)
	return append(buf, crlf...)
}

// EncodeSimpleString returns +OK\r\n
func EncodeSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

// EncodeError returns -ERR msg\r\n
func EncodeError(msg string) []byte {
	return []byte("-ERR " + msg + "\r\n")
}

// EncodeInteger returns :N\r\n
func EncodeInteger(n int) []byte {
	return []byte(":" + strconv.Itoa(n) + "\r\n")
}

// EncodeBulkString returns $len\r\ncontent\r\n or $-1\r\n for nil
func EncodeBulkString(s []byte) []byte {
	if s == nil {
		return []byte("$-1\r\n")
	}
	return append(append(append([]byte("$"+strconv.Itoa(len(s))), crlf...), s...), crlf...)
}

// EncodeArray encodes an array of items
func EncodeArray(items [][]byte) []byte {
	// Pre-allocate: "*N\r\n" + each "$L\r\n" + content + "\r\n"
	size := 4 + len(crlf)
	for _, item := range items {
		if item == nil {
			size += 5 + len(crlf)
		} else {
			size += 5 + len(item) + len(crlf) // 5 for "$9999"
		}
	}
	buf := make([]byte, 0, size)
	buf = append(buf, '*')
	buf = append(buf, strconv.Itoa(len(items))...)
	buf = append(buf, crlf...)
	for _, item := range items {
		buf = appendBulkString(buf, item)
	}
	return buf
}

func appendBulkString(buf []byte, s []byte) []byte {
	if s == nil {
		return append(buf, '$', '-', '1', '\r', '\n')
	}
	buf = append(buf, '$')
	buf = append(buf, strconv.Itoa(len(s))...)
	buf = append(buf, crlf...)
	buf = append(buf, s...)
	return append(buf, crlf...)
}

// Reader parses RESP2 protocol incrementally
type Reader struct {
	buf []byte
}

// Feed adds data to the buffer
func (r *Reader) Feed(data []byte) {
	r.buf = append(r.buf, data...)
}

// ParseCommands returns complete command arrays from buffer (each command is [][]byte)
func (r *Reader) ParseCommands() [][][]byte {
	var commands [][][]byte
	for len(r.buf) > 0 {
		items, consumed, err := r.parseArray()
		if err != nil || consumed == 0 {
			break
		}
		if items != nil {
			commands = append(commands, items)
		}
		r.buf = r.buf[consumed:]
	}
	return commands
}

// parseArray parses RESP array *N\r\n$L1\r\n...$Ln\r\n...; $len=-1 means null bulk (empty item).
func (r *Reader) parseArray() ([][]byte, int, error) {
	if len(r.buf) < 1 || r.buf[0] != '*' {
		return nil, 0, fmt.Errorf("expected array")
	}
	idx := bytes.Index(r.buf, crlf)
	if idx == -1 {
		return nil, 0, fmt.Errorf("need more data")
	}
	count, err := strconv.Atoi(string(r.buf[1:idx]))
	if err != nil {
		return nil, 0, err
	}
	consumed := idx + len(crlf)
	remaining := r.buf[consumed:]
	var items [][]byte
	for i := 0; i < count; i++ {
		if len(remaining) < 4 {
			return nil, 0, fmt.Errorf("need more data")
		}
		if remaining[0] != '$' {
			return nil, 0, fmt.Errorf("expected bulk string")
		}
		crlfPos := bytes.Index(remaining, crlf)
		if crlfPos == -1 {
			return nil, 0, fmt.Errorf("need more data")
		}
		length, _ := strconv.Atoi(string(remaining[1:crlfPos]))
		consumed += crlfPos + len(crlf)
		if length == -1 {
			// Null bulk string: $-1\r\n → treat as empty item
			items = append(items, []byte{})
		} else {
			bulkEnd := crlfPos + len(crlf) + length + len(crlf)
			if len(remaining) < bulkEnd {
				return nil, 0, fmt.Errorf("need more data")
			}
			items = append(items, remaining[crlfPos+len(crlf):crlfPos+len(crlf)+length])
			consumed += length + len(crlf)
		}
		remaining = r.buf[consumed:]
	}
	return items, consumed, nil
}

// ResponseReader reads complete RESP2 responses from a connection.
type ResponseReader struct {
	conn io.Reader
	buf  []byte
}

// NewResponseReader creates a reader for parsing RESP responses.
func NewResponseReader(conn io.Reader) *ResponseReader {
	return &ResponseReader{conn: conn, buf: nil}
}

// ReadResponse reads one complete RESP response and returns it as raw bytes.
func (r *ResponseReader) ReadResponse() ([]byte, error) {
	for {
		resp, consumed, err := r.tryParseOne(r.buf)
		if err == errNeedMore {
			chunk := make([]byte, 4096)
			n, readErr := r.conn.Read(chunk)
			if n > 0 {
				r.buf = append(r.buf, chunk[:n]...)
			}
			if readErr != nil {
				if len(r.buf) > 0 {
					return nil, readErr
				}
				return nil, readErr
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		r.buf = r.buf[consumed:]
		return resp, nil
	}
}

var errNeedMore = fmt.Errorf("need more data")

// FormatResponse converts raw RESP bytes to a human-readable string for display.
func FormatResponse(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	// Trim trailing \r\n for display
	trimmed := bytes.TrimSuffix(raw, crlf)
	if len(trimmed) < 2 {
		return string(raw)
	}
	switch trimmed[0] {
	case '+':
		return string(trimmed[1:])
	case '-':
		return string(trimmed)
	case ':':
		return string(trimmed[1:])
	case '$':
		idx := bytes.IndexByte(trimmed, '\r')
		var length int
		if idx >= 0 {
			length, _ = strconv.Atoi(string(trimmed[1:idx]))
		} else if len(trimmed) > 1 {
			length, _ = strconv.Atoi(string(trimmed[1:]))
		}
		if length < 0 {
			return "(nil)"
		}
		if idx < 0 {
			return string(trimmed)
		}
		content := trimmed[idx+2:]
		if len(content) < length {
			length = len(content)
		}
		return string(content[:length])
	case '*':
		idx := bytes.IndexByte(trimmed, '\r')
		if idx < 0 {
			return string(trimmed)
		}
		count, _ := strconv.Atoi(string(trimmed[1:idx]))
		if count < 0 {
			return "(nil)"
		}
		// Use raw (not trimmed) so last element's trailing \r\n is preserved
		rest := raw
		if len(rest) > idx+2 {
			rest = rest[idx+2:]
		} else {
			rest = nil
		}
		var sb strings.Builder
		for i := 0; i < count && len(rest) > 0; i++ {
			item, consumed := parseOneValue(rest)
			if consumed == 0 || consumed > len(rest) {
				break
			}
			if i > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(FormatResponse(item))
			rest = rest[consumed:]
		}
		return sb.String()
	}
	return string(trimmed)
}

// parseOneValue parses one complete RESP value from data, returns (raw value bytes, consumed).
func parseOneValue(data []byte) ([]byte, int) {
	if len(data) < 3 {
		return nil, 0
	}
	switch data[0] {
	case '+', '-', ':':
		idx := bytes.Index(data, crlf)
		if idx == -1 {
			return nil, 0
		}
		return data[:idx+len(crlf)], idx + len(crlf)
	case '$':
		idx := bytes.Index(data, crlf)
		if idx == -1 {
			return nil, 0
		}
		length, err := strconv.Atoi(string(data[1:idx]))
		if err != nil {
			return nil, 0
		}
		consumed := idx + len(crlf)
		if length == -1 {
			return data[:consumed], consumed
		}
		need := length + len(crlf)
		if consumed+need > len(data) {
			return nil, 0
		}
		consumed += need
		return data[:consumed], consumed
	case '*':
		idx := bytes.Index(data, crlf)
		if idx == -1 {
			return nil, 0
		}
		count, err := strconv.Atoi(string(data[1:idx]))
		if err != nil {
			return nil, 0
		}
		consumed := idx + len(crlf)
		for i := 0; i < count; i++ {
			_, c := parseOneValue(data[consumed:])
			if c == 0 {
				return nil, 0
			}
			consumed += c
		}
		return data[:consumed], consumed
	}
	return nil, 0
}

// tryParseOne parses one complete RESP value recursively (handles nested arrays).
func (r *ResponseReader) tryParseOne(data []byte) (response []byte, consumed int, err error) {
	if len(data) < 3 {
		return nil, 0, errNeedMore
	}
	switch data[0] {
	case '+', '-', ':':
		idx := bytes.Index(data, crlf)
		if idx == -1 {
			return nil, 0, errNeedMore
		}
		return data[:idx+len(crlf)], idx + len(crlf), nil
	case '$':
		idx := bytes.Index(data, crlf)
		if idx == -1 {
			return nil, 0, errNeedMore
		}
		length, err := strconv.Atoi(string(data[1:idx]))
		if err != nil {
			return nil, 0, err
		}
		consumed = idx + len(crlf)
		if length == -1 {
			return data[:consumed], consumed, nil
		}
		need := consumed + length + len(crlf)
		if len(data) < need {
			return nil, 0, errNeedMore
		}
		return data[:need], need, nil
	case '*':
		idx := bytes.Index(data, crlf)
		if idx == -1 {
			return nil, 0, errNeedMore
		}
		count, err := strconv.Atoi(string(data[1:idx]))
		if err != nil {
			return nil, 0, err
		}
		consumed = idx + len(crlf)
		remaining := data[consumed:]
		for i := 0; i < count; i++ {
			_, c, err := r.tryParseOne(remaining)
			if err != nil {
				return nil, 0, err
			}
			consumed += c
			remaining = remaining[c:]
		}
		return data[:consumed], consumed, nil
	default:
		return nil, 0, fmt.Errorf("invalid RESP type: %c", data[0])
	}
}
