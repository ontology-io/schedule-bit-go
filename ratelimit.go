package schedulebit

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RateLimit is an answer's RateLimit-Limit, RateLimit-Remaining and
// RateLimit-Reset headers. Present is false when the answer carried none.
type RateLimit struct {
	Present   bool
	Limit     int
	Remaining int
	Reset     time.Duration // until the window resets
}

func parseRateLimit(h http.Header) RateLimit {
	var r RateLimit
	num := func(name string) (float64, bool) {
		v := strings.TrimSpace(h.Get(name))
		if v == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	}
	if v, ok := num("RateLimit-Limit"); ok {
		r.Present, r.Limit = true, int(v)
	}
	if v, ok := num("RateLimit-Remaining"); ok {
		r.Present, r.Remaining = true, int(v)
	}
	if v, ok := num("RateLimit-Reset"); ok {
		r.Present, r.Reset = true, time.Duration(v*float64(time.Second))
	}
	return r
}
