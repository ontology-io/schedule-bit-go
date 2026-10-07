package schedulebit

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ontology-io/schedule-bit-go/api"
)

// transport is the one place every request passes: it signs the request,
// keys writes, spaces requests, retries what may be retried, and turns a
// failed answer into a typed error. It is the generated client's
// HttpRequestDoer, so every generated method gets the same behaviour.
type transport struct {
	cfg    *config
	bucket *bucket
}

type route struct {
	op api.Operation
	re *regexp.Regexp
}

var routes = func() []route {
	rs := make([]route, 0, len(api.Operations))
	for _, op := range api.Operations {
		// QuoteMeta escapes the braces too: `\{w\}` stands for one path segment.
		pattern := placeholder.ReplaceAllString(regexp.QuoteMeta(op.Path), `[^/]+`)
		rs = append(rs, route{op, regexp.MustCompile(`^` + pattern + `$`)})
	}
	return rs
}()

var placeholder = regexp.MustCompile(`\\\{[^}\\]+\\\}`)

// operationOf finds the contract operation a request is for, by its escaped
// path (a World named workspace/world travels as one segment).
func operationOf(method, path string) (api.Operation, bool) {
	for _, r := range routes {
		if r.op.Method == method && r.re.MatchString(path) {
			return r.op, true
		}
	}
	return api.Operation{}, false
}

type callKey struct{}

// callOptions travel on a request's context: a caller's own idempotency key,
// or a per-call timeout.
type callOptions struct {
	idempotencyKey string
	timeout        time.Duration
}

// WithIdempotencyKey makes the next call through ctx send this key instead of
// a fresh one.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	o := optionsOf(ctx)
	o.idempotencyKey = key
	return context.WithValue(ctx, callKey{}, o)
}

// WithCallTimeout bounds each attempt of calls made with ctx (the client's
// default is 30 s); a negative d means no per-attempt bound, only ctx's.
func WithCallTimeout(ctx context.Context, d time.Duration) context.Context {
	o := optionsOf(ctx)
	o.timeout = d
	return context.WithValue(ctx, callKey{}, o)
}

func optionsOf(ctx context.Context) callOptions {
	o, _ := ctx.Value(callKey{}).(callOptions)
	return o
}

func (t *transport) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	op, known := operationOf(req.Method, req.URL.EscapedPath())
	call := optionsOf(ctx)

	if t.cfg.key != "" && req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", "Bearer "+t.cfg.key)
	}
	req.Header.Set("User-Agent", userAgent)
	if known && op.Keyed && req.Header.Get("Idempotency-Key") == "" {
		key := call.idempotencyKey
		if key == "" {
			key = newUUID()
		}
		req.Header.Set("Idempotency-Key", key)
	}
	repeatable := !known && (req.Method == http.MethodGet || req.Method == http.MethodHead)
	if known {
		repeatable = op.Repeatable
	}
	timeout := t.cfg.timeout
	if call.timeout != 0 { // a negative per-call timeout means none
		timeout = call.timeout
	}
	name := req.Method + " " + req.URL.Path
	if known {
		name = op.ID
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		if t.bucket != nil {
			if err := t.bucket.wait(ctx, t.cfg.sleep); err != nil {
				return nil, err
			}
		}
		attemptReq, cancel, err := t.prepare(req, timeout)
		if err != nil {
			return nil, err
		}
		resp, err := t.cfg.http.Do(attemptReq)
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			if !repeatable || attempt >= t.cfg.maxRetries || !transient(err) {
				return nil, &NetworkError{Operation: name, Attempts: attempt + 1, Err: redactErr(err)}
			}
			if err := t.cfg.sleep(ctx, t.backoff(attempt, nil)); err != nil {
				return nil, err
			}
			continue
		}
		limit := parseRateLimit(resp.Header)
		if limit.Present && t.cfg.onRateLimit != nil {
			t.cfg.onRateLimit(limit)
		}
		if resp.StatusCode < 400 {
			resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			return resp, nil
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		retry := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable ||
			(resp.StatusCode >= 500 && repeatable)
		if !retry || attempt >= t.cfg.maxRetries {
			return nil, errorOf(name, resp, body, attempt+1)
		}
		lastErr = errorOf(name, resp, body, attempt+1)
		if err := t.cfg.sleep(ctx, t.backoff(attempt, resp.Header)); err != nil {
			return nil, errors.Join(err, lastErr)
		}
	}
}

// prepare copies the request for one attempt, with a fresh body and its own
// timeout. The cancel func ends the attempt; for a success it runs when the
// caller closes the body.
func (t *transport) prepare(req *http.Request, timeout time.Duration) (*http.Request, context.CancelFunc, error) {
	ctx, cancel := req.Context(), context.CancelFunc(func() {})
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	r := req.Clone(ctx)
	if req.Body != nil && req.Body != http.NoBody {
		if req.GetBody == nil {
			b, err := io.ReadAll(req.Body)
			if err != nil {
				cancel()
				return nil, nil, err
			}
			req.Body.Close()
			req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
		}
		body, err := req.GetBody()
		if err != nil {
			cancel()
			return nil, nil, err
		}
		r.Body = body
	}
	return r, cancel, nil
}

// backoff is the wait before the next attempt: the server's hint when it
// gave one (Retry-After, else RateLimit-Reset), else full jitter over an
// exponential ceiling; capped either way.
func (t *transport) backoff(attempt int, h http.Header) time.Duration {
	if hint, ok := serverHint(h, t.cfg.now()); ok {
		return min(hint, t.cfg.backoffCap)
	}
	ceiling := float64(t.cfg.backoffBase) * math.Pow(2, float64(attempt))
	ceiling = math.Min(ceiling, float64(t.cfg.backoffCap))
	return time.Duration(t.cfg.random() * ceiling)
}

func serverHint(h http.Header, now time.Time) (time.Duration, bool) {
	if h == nil {
		return 0, false
	}
	if v := strings.TrimSpace(h.Get("Retry-After")); v != "" {
		if secs, err := strconv.ParseFloat(v, 64); err == nil && secs >= 0 {
			return time.Duration(secs * float64(time.Second)), true
		}
		if at, err := http.ParseTime(v); err == nil {
			return max(at.Sub(now), 0), true
		}
	}
	if v := strings.TrimSpace(h.Get("RateLimit-Reset")); v != "" {
		if secs, err := strconv.ParseFloat(v, 64); err == nil && secs >= 0 {
			return time.Duration(secs * float64(time.Second)), true
		}
	}
	return 0, false
}

// transient: worth another try (a reset, a refusal to connect, DNS, a timed-out attempt).
func transient(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	var dns *net.DNSError
	var op *net.OpError
	return errors.As(err, &dns) || errors.As(err, &op) || strings.Contains(err.Error(), "connection reset")
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func defaultRandom() float64 { return mrand.Float64() }

func defaultSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// bucket is the optional client-side token bucket: it spaces requests so a
// client stays under a per-minute rate (the plan's, say) instead of being told
// to wait by 429s.
type bucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	perSec   float64
	last     time.Time
	now      func() time.Time
}

func newBucket(perMinute int, now func() time.Time) *bucket {
	c := float64(perMinute)
	return &bucket{capacity: c, tokens: c, perSec: c / 60, last: now(), now: now}
}

func (b *bucket) wait(ctx context.Context, sleep func(context.Context, time.Duration) error) error {
	for {
		b.mu.Lock()
		now := b.now()
		b.tokens = math.Min(b.capacity, b.tokens+now.Sub(b.last).Seconds()*b.perSec)
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		need := time.Duration((1 - b.tokens) / b.perSec * float64(time.Second))
		b.mu.Unlock()
		if err := sleep(ctx, need); err != nil {
			return err
		}
	}
}
