package origin

import (
	"io"
	"net/http"
)

// maxHealthBodyDrain bounds how much of a health-check body is read before the
// connection is returned to the pool. Draining is what makes keep-alive work;
// bounding the drain is what stops a hostile or broken origin from streaming
// unbounded data at the health checker.
const maxHealthBodyDrain = 4 << 10

// drainBody drains up to maxHealthBodyDrain bytes of resp.Body.
func drainBody(resp *http.Response) (int64, error) {
	return io.Copy(io.Discard, io.LimitReader(resp.Body, maxHealthBodyDrain))
}
