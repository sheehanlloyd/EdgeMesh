// Package errs defines EdgeMesh's error classification scheme.
//
// Errors are classified rather than string-matched. Every error that crosses a
// subsystem boundary carries a Class so that callers, metrics, and HTTP/gRPC
// status mapping can make decisions without inspecting messages.
package errs

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// Class is a coarse, stable category for an error. It is the only part of an
// error that control flow may branch on.
type Class string

const (
	ClassUnknown       Class = "unknown"
	ClassValidation    Class = "validation"
	ClassNotFound      Class = "not_found"
	ClassConflict      Class = "conflict"
	ClassNotLeader     Class = "not_leader"
	ClassUnavailable   Class = "unavailable"
	ClassTimeout       Class = "timeout"
	ClassCanceled      Class = "canceled"
	ClassRateLimited   Class = "rate_limited"
	ClassCircuitOpen   Class = "circuit_open"
	ClassOriginFailure Class = "origin_failure"
	ClassPeerFailure   Class = "peer_failure"
	ClassCacheMiss     Class = "cache_miss"
	ClassStorage       Class = "storage"
	ClassProtocol      Class = "protocol"
	ClassUnauthorized  Class = "unauthorized"
	ClassTooLarge      Class = "too_large"
)

// Sentinels for the classes that are commonly compared directly. Wrapping
// preserves the class, so errors.Is against these works through %w chains.
var (
	ErrValidation    = New(ClassValidation, "validation failed")
	ErrNotFound      = New(ClassNotFound, "not found")
	ErrConflict      = New(ClassConflict, "version conflict")
	ErrNotLeader     = New(ClassNotLeader, "not the raft leader")
	ErrUnavailable   = New(ClassUnavailable, "unavailable")
	ErrTimeout       = New(ClassTimeout, "timed out")
	ErrCanceled      = New(ClassCanceled, "canceled")
	ErrRateLimited   = New(ClassRateLimited, "rate limited")
	ErrCircuitOpen   = New(ClassCircuitOpen, "circuit breaker open")
	ErrOriginFailure = New(ClassOriginFailure, "origin failure")
	ErrPeerFailure   = New(ClassPeerFailure, "peer failure")
	ErrCacheMiss     = New(ClassCacheMiss, "cache miss")
	ErrStorage       = New(ClassStorage, "storage failure")
	ErrProtocol      = New(ClassProtocol, "protocol violation")
	ErrUnauthorized  = New(ClassUnauthorized, "unauthorized")
	ErrTooLarge      = New(ClassTooLarge, "object too large")
)

// Error is a classified error. It participates in errors.Is/As chains.
type Error struct {
	class Class
	msg   string
	cause error
}

// New builds a classified error with no cause.
func New(class Class, format string, args ...any) *Error {
	return &Error{class: class, msg: fmt.Sprintf(format, args...)}
}

// Wrap attaches a class and message to an existing error, preserving the chain.
// A nil cause yields nil so callers can wrap unconditionally.
func Wrap(class Class, cause error, format string, args ...any) error {
	if cause == nil {
		return nil
	}
	return &Error{class: class, msg: fmt.Sprintf(format, args...), cause: cause}
}

func (e *Error) Error() string {
	if e.cause == nil {
		return e.msg
	}
	return e.msg + ": " + e.cause.Error()
}

// Unwrap exposes the cause to errors.Is/As.
func (e *Error) Unwrap() error { return e.cause }

// Class reports this error's classification.
func (e *Error) Class() Class { return e.class }

// Is treats two classified errors as equal when their classes match. This is
// what makes errors.Is(err, ErrTimeout) work for any timeout error regardless
// of its message or cause.
func (e *Error) Is(target error) bool {
	var t *Error
	if errors.As(target, &t) {
		return t.class == e.class
	}
	return false
}

// Classified is implemented by any error that carries a Class. Types outside
// this package satisfy it to participate in classification without wrapping an
// *Error, which is what lets a package define an error type carrying extra
// structured detail (a leader hint, say) that still maps to the right HTTP
// status.
type Classified interface {
	Class() Class
}

// ClassOf extracts the classification of err, walking the wrap chain.
//
// A classified error wins. Failing that, the standard context errors are
// recognized: they reach this package unwrapped from every timeout and
// cancellation path in the standard library, and leaving them unclassified
// would map a plain request timeout onto a generic 502 instead of a 504.
//
// Errors that carry no class report ClassUnknown; nil reports the empty class.
func ClassOf(err error) Class {
	if err == nil {
		return ""
	}
	var c Classified
	if errors.As(err, &c) {
		return c.Class()
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return ClassTimeout
	case errors.Is(err, context.Canceled):
		return ClassCanceled
	}
	// A net.Error that reports a timeout is the same condition arriving by a
	// different route.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ClassTimeout
	}
	return ClassUnknown
}

// IsClass reports whether err is classified as class anywhere in its chain.
func IsClass(err error, class Class) bool { return ClassOf(err) == class }
