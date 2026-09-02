package proxy

import (
	"errors"
	"net"
)

// asNetError is errors.As specialized to net.Error, kept separate so the
// classification helper reads cleanly.
func asNetError(err error, target *net.Error) bool { return errors.As(err, target) }
