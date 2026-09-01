package statemachine

import (
	"encoding/binary"
	"math"

	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

// The command envelope uses a minimal length-prefixed framing rather than a
// protobuf message so the framing itself has no schema-evolution surface: every
// field is fixed-width or explicitly length-prefixed, decoded in a fixed order,
// and every read is bounds-checked. Malformed input from a corrupted log or a
// hostile peer must produce an error, never a panic or an out-of-range read.

// maxFieldBytes bounds a single length-prefixed field. A corrupted length
// prefix must not cause a multi-gigabyte allocation.
const maxFieldBytes = 64 << 20

type encoder struct{ buf []byte }

func newEncoder() *encoder { return &encoder{buf: make([]byte, 0, 256)} }

func (e *encoder) uint64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	e.buf = append(e.buf, b[:]...)
}

func (e *encoder) int64(v int64) { e.uint64(uint64(v)) }

func (e *encoder) int32(v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	e.buf = append(e.buf, b[:]...)
}

func (e *encoder) bytes(v []byte) {
	e.uint64(uint64(len(v)))
	e.buf = append(e.buf, v...)
}

func (e *encoder) string(v string) { e.bytes([]byte(v)) }

func (e *encoder) result() []byte { return e.buf }

type decoder struct {
	buf []byte
	pos int
}

func newDecoder(b []byte) *decoder { return &decoder{buf: b} }

var errTruncated = errs.New(errs.ClassProtocol, "encoded record is truncated")

func (d *decoder) uint64() (uint64, error) {
	if d.pos+8 > len(d.buf) {
		return 0, errTruncated
	}
	v := binary.BigEndian.Uint64(d.buf[d.pos:])
	d.pos += 8
	return v, nil
}

func (d *decoder) int64() (int64, error) {
	v, err := d.uint64()
	return int64(v), err
}

func (d *decoder) int32() (int32, error) {
	if d.pos+4 > len(d.buf) {
		return 0, errTruncated
	}
	v := binary.BigEndian.Uint32(d.buf[d.pos:])
	d.pos += 4
	return int32(v), nil
}

func (d *decoder) bytes() ([]byte, error) {
	n, err := d.uint64()
	if err != nil {
		return nil, err
	}
	if n > maxFieldBytes {
		return nil, errs.New(errs.ClassProtocol,
			"encoded field claims %d bytes, above the %d byte bound", n, maxFieldBytes)
	}
	if n > math.MaxInt32 || d.pos+int(n) > len(d.buf) {
		return nil, errTruncated
	}
	if n == 0 {
		return nil, nil
	}
	// The slice is copied so a retained result does not pin the whole record.
	out := make([]byte, n)
	copy(out, d.buf[d.pos:d.pos+int(n)])
	d.pos += int(n)
	return out, nil
}

func (d *decoder) string() (string, error) {
	b, err := d.bytes()
	return string(b), err
}
