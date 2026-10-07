// Package schedulebit is the Go client for the schedule-bit API.
//
// Every operation of the API is a generated method on Client (from the
// embedded api.ClientWithResponses), so the client is never behind the
// contract. What the generator cannot know lives here: retries with back-off
// that honour the server's hints, idempotency keys on writes, rate-limit
// headers and an optional client-side limit, per-attempt timeouts, typed
// errors (APIError, Refusal, LocalTimeError, NetworkError), masks as bit sets,
// local time, the live watch socket and the Director's event stream.
//
//	c, err := schedulebit.New(schedulebit.WithAPIKey(os.Getenv("SCHEDULE_BIT_API_KEY")))
//	worlds, err := c.ListWorldsWithResponse(ctx)
package schedulebit

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ontology-io/schedule-bit-go/api"
)

// Version of this SDK; sent in the User-Agent.
const Version = "0.1.0"

const userAgent = "schedule-bit-go/" + Version

// DefaultBaseURL is the production API.
const DefaultBaseURL = "https://api.schedule-bit.io"

// Client is the schedule-bit API client. Its generated methods (one per
// operation, e.g. ClaimWithResponse) all pass through the same transport.
type Client struct {
	*api.ClientWithResponses
	cfg *config
}

type config struct {
	base        string
	key         string
	http        *http.Client
	timeout     time.Duration
	maxRetries  int
	backoffBase time.Duration
	backoffCap  time.Duration
	perMinute   int
	onRateLimit func(RateLimit)
	sleep       func(context.Context, time.Duration) error
	random      func() float64
	now         func() time.Time
}

// Option configures a Client.
type Option func(*config)

// WithBaseURL points the client at another deployment (dev, staging, a local server).
func WithBaseURL(u string) Option { return func(c *config) { c.base = strings.TrimRight(u, "/") } }

// WithAPIKey sets the key; without it the client reads SCHEDULE_BIT_API_KEY.
func WithAPIKey(k string) Option { return func(c *config) { c.key = k } }

// WithHTTPClient replaces the underlying *http.Client (its own Timeout should
// stay zero: the SDK times each attempt itself).
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.http = h } }

// WithTimeout bounds each attempt (default 30 s; 0 for none).
func WithTimeout(d time.Duration) Option { return func(c *config) { c.timeout = d } }

// WithMaxRetries sets how many times a failed attempt is retried (default 4; 0 turns retrying off).
func WithMaxRetries(n int) Option { return func(c *config) { c.maxRetries = max(n, 0) } }

// WithBackoff sets the base and the cap of the back-off (default 500 ms and 30 s).
// The cap also bounds how long a server hint is honoured.
func WithBackoff(base, maxWait time.Duration) Option {
	return func(c *config) { c.backoffBase, c.backoffCap = base, maxWait }
}

// WithRateLimit turns on the client-side token bucket: at most perMinute
// requests a minute leave this client (off by default).
func WithRateLimit(perMinute int) Option { return func(c *config) { c.perMinute = perMinute } }

// OnRateLimit is called with the RateLimit-* headers of every answer that carries them.
func OnRateLimit(f func(RateLimit)) Option { return func(c *config) { c.onRateLimit = f } }

// WithSleep replaces the waiting between attempts (tests pass one that records and returns at once).
func WithSleep(f func(context.Context, time.Duration) error) Option {
	return func(c *config) { c.sleep = f }
}

// WithRandom replaces the jitter source; it returns a float in [0, 1).
func WithRandom(f func() float64) Option { return func(c *config) { c.random = f } }

// WithClock replaces the clock (for Retry-After dates and the token bucket).
func WithClock(f func() time.Time) Option { return func(c *config) { c.now = f } }

// New makes a client. With no key given it reads SCHEDULE_BIT_API_KEY; a
// client with no key can still call the open operations (health, presets,
// sandbox).
func New(opts ...Option) (*Client, error) {
	cfg := &config{
		base:        DefaultBaseURL,
		http:        &http.Client{},
		timeout:     30 * time.Second,
		maxRetries:  4,
		backoffBase: 500 * time.Millisecond,
		backoffCap:  30 * time.Second,
		sleep:       defaultSleep,
		random:      defaultRandom,
		now:         time.Now,
	}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.key == "" {
		cfg.key = os.Getenv("SCHEDULE_BIT_API_KEY")
	}
	if cfg.base == "" {
		return nil, errors.New("schedulebit: empty base URL")
	}
	t := &transport{cfg: cfg}
	if cfg.perMinute > 0 {
		t.bucket = newBucket(cfg.perMinute, cfg.now)
	}
	gen, err := api.NewClientWithResponses(cfg.base, api.WithHTTPClient(t))
	if err != nil {
		return nil, err
	}
	return &Client{ClientWithResponses: gen, cfg: cfg}, nil
}

// BaseURL is the deployment this client talks to.
func (c *Client) BaseURL() string { return c.cfg.base }

// doer is the transport, for hand-written calls (the watch, the stream).
func (c *Client) doer() *transport {
	t := &transport{cfg: c.cfg}
	if c.cfg.perMinute > 0 {
		t.bucket = newBucket(c.cfg.perMinute, c.cfg.now)
	}
	return t
}

// TryClaim claims, returning a refusal as a value instead of an error: the
// engine saying no is an expected outcome. err is for everything else.
func (c *Client) TryClaim(ctx context.Context, world string, body api.ClaimJSONRequestBody) (*api.Receipt, *Refusal, error) {
	resp, err := c.ClaimWithResponse(ctx, world, nil, body)
	var refusal *Refusal
	if errors.As(err, &refusal) {
		return nil, refusal, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return resp.JSON201, nil, nil
}
