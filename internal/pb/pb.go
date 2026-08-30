// Package pb holds small helpers over generated protobuf types.
//
// It exists so that behaviour every package needs, deep copying while
// preserving the concrete type, is written and justified once rather than
// repeated with a bare type assertion at each call site.
package pb

import "google.golang.org/protobuf/proto"

// Clone returns a deep copy of m, preserving its concrete type.
//
// proto.Clone returns the proto.Message interface, so every caller would
// otherwise need its own type assertion. The assertion here cannot fail:
// proto.Clone is documented to return a message of the same concrete type as
// its argument. Centralizing it means that reasoning is recorded once.
//
// Copies matter throughout EdgeMesh because replicated state is shared with
// readers: handing out the stored pointer would let a caller mutate the state
// machine's contents, and the mutation would exist only on that one replica.
func Clone[T proto.Message](m T) T {
	//nolint:errcheck // Cannot fail: proto.Clone preserves the concrete type.
	return proto.Clone(m).(T)
}
