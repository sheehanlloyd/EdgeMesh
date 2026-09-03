package main

import (
	"encoding/json"
	"strconv"
)

// protoInt64 decodes a 64-bit integer that protojson renders as a JSON string.
//
// The protobuf JSON mapping encodes int64 and uint64 as strings because
// JavaScript numbers cannot represent the full 64-bit range exactly. Fields
// coming from the state machine therefore arrive quoted, while fields the admin
// API constructs itself arrive as numbers. Accepting both keeps the CLI's
// decoding independent of which side produced a given field.
type protoInt64 int64

func (v *protoInt64) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*v = 0
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s == "" {
			*v = 0
			return nil
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		*v = protoInt64(n)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*v = protoInt64(n)
	return nil
}

// Int64 returns the decoded value.
func (v protoInt64) Int64() int64 { return int64(v) }

// protoUint64 is the unsigned counterpart.
type protoUint64 uint64

func (v *protoUint64) UnmarshalJSON(b []byte) error {
	var signed protoInt64
	if err := signed.UnmarshalJSON(b); err != nil {
		return err
	}
	if signed < 0 {
		*v = 0
		return nil
	}
	*v = protoUint64(signed)
	return nil
}

// Uint64 returns the decoded value.
func (v protoUint64) Uint64() uint64 { return uint64(v) }
