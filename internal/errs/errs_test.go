package errs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestClassOfClassifiedErrors(t *testing.T) {
	if got := ClassOf(nil); got != "" {
		t.Fatalf("ClassOf(nil) = %q, want the empty class", got)
	}
	if got := ClassOf(New(ClassTimeout, "boom")); got != ClassTimeout {
		t.Fatalf("ClassOf = %q", got)
	}
	// Classification must survive wrapping, which is the whole point of using
	// %w throughout.
	wrapped := fmt.Errorf("outer: %w", Wrap(ClassStorage, errors.New("inner"), "middle"))
	if got := ClassOf(wrapped); got != ClassStorage {
		t.Fatalf("ClassOf through a wrap chain = %q, want storage", got)
	}
}

// The standard context errors reach this package unwrapped from every timeout
// path in the standard library. Leaving them unclassified would map a plain
// request timeout onto a generic 502 rather than a 504.
func TestClassOfStandardContextErrors(t *testing.T) {
	if got := ClassOf(context.DeadlineExceeded); got != ClassTimeout {
		t.Fatalf("ClassOf(DeadlineExceeded) = %q, want timeout", got)
	}
	if got := ClassOf(context.Canceled); got != ClassCanceled {
		t.Fatalf("ClassOf(Canceled) = %q, want canceled", got)
	}
	// And through a wrap chain.
	if got := ClassOf(fmt.Errorf("fetching: %w", context.DeadlineExceeded)); got != ClassTimeout {
		t.Fatalf("ClassOf through a wrap = %q, want timeout", got)
	}
}

func TestClassOfNetTimeout(t *testing.T) {
	err := &net.DNSError{Err: "timed out", IsTimeout: true}
	if got := ClassOf(err); got != ClassTimeout {
		t.Fatalf("ClassOf(net timeout) = %q, want timeout", got)
	}
	// A non-timeout network error stays unknown rather than being mislabelled.
	if got := ClassOf(&net.DNSError{Err: "no such host"}); got != ClassUnknown {
		t.Fatalf("ClassOf(non-timeout DNS error) = %q, want unknown", got)
	}
}

func TestClassOfUnclassified(t *testing.T) {
	if got := ClassOf(errors.New("plain")); got != ClassUnknown {
		t.Fatalf("ClassOf = %q, want unknown", got)
	}
}

// errors.Is must match on class, so a caller can test for "any timeout" without
// knowing which subsystem produced it.
func TestIsMatchesOnClass(t *testing.T) {
	err := Wrap(ClassTimeout, errors.New("inner"), "origin %q", "o1")
	if !errors.Is(err, ErrTimeout) {
		t.Fatal("a timeout-classified error must match ErrTimeout")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("a timeout error must not match ErrNotFound")
	}
	// The underlying cause stays reachable.
	if err.Error() == "" {
		t.Fatal("the error message was lost")
	}
}

func TestWrapNilIsNil(t *testing.T) {
	// Wrapping unconditionally is a common pattern; it must not manufacture an
	// error where there was none.
	if err := Wrap(ClassStorage, nil, "context"); err != nil {
		t.Fatalf("Wrap(nil) = %v, want nil", err)
	}
}

func TestIsClass(t *testing.T) {
	if !IsClass(New(ClassConflict, "x"), ClassConflict) {
		t.Fatal("IsClass failed on a matching class")
	}
	if IsClass(New(ClassConflict, "x"), ClassTimeout) {
		t.Fatal("IsClass matched a different class")
	}
	if IsClass(nil, ClassUnknown) {
		t.Fatal("IsClass(nil) must not match a class")
	}
}

// A custom error type outside this package participates by implementing
// Classified, which is what lets the admin API carry a leader hint while still
// mapping to the right HTTP status.
type customError struct{}

func (customError) Error() string { return "custom" }
func (customError) Class() Class  { return ClassNotLeader }

func TestExternalTypesCanClassify(t *testing.T) {
	if got := ClassOf(customError{}); got != ClassNotLeader {
		t.Fatalf("ClassOf(custom) = %q, want not_leader", got)
	}
	if got := ClassOf(fmt.Errorf("wrapped: %w", customError{})); got != ClassNotLeader {
		t.Fatalf("ClassOf(wrapped custom) = %q", got)
	}
}
