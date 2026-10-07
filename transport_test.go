package schedulebit

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ontology-io/schedule-bit-go/api"
)

// fake is a scripted server: each request gets the next answer in line.
type fake struct {
	mu       sync.Mutex
	answers  []func(w http.ResponseWriter, r *http.Request)
	requests []*http.Request
	bodies   []string
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	b, _ := io.ReadAll(r.Body)
	f.requests = append(f.requests, r.Clone(context.Background()))
	f.bodies = append(f.bodies, string(b))
	i := len(f.requests) - 1
	var answer func(http.ResponseWriter, *http.Request)
	if i < len(f.answers) {
		answer = f.answers[i]
	} else {
		answer = f.answers[len(f.answers)-1]
	}
	f.mu.Unlock()
	answer(w, r)
}

func status(code int, headers ...string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, `{"type":"about:blank","title":"t","status":`+http.StatusText(code)[:0]+`0,"detail":"d"}`)
	}
}

func ok(body string, headers ...string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	}
}

func created(body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, body)
	}
}

type recorder struct {
	mu     sync.Mutex
	sleeps []time.Duration
}

func (r *recorder) sleep(ctx context.Context, d time.Duration) error {
	r.mu.Lock()
	r.sleeps = append(r.sleeps, d)
	r.mu.Unlock()
	return ctx.Err()
}

var epoch = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func client(t *testing.T, f *fake, opts ...Option) (*Client, *recorder) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	rec := &recorder{}
	base := []Option{
		WithBaseURL(srv.URL), WithAPIKey("sbk_testsecret123"),
		WithSleep(rec.sleep), WithRandom(func() float64 { return 0.5 }),
		WithClock(func() time.Time { return epoch }),
	}
	c, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return c, rec
}

const health = `{"ok":true,"service":"schedule-bit","version":"x","presets":[]}`

func TestRetriesServiceUnavailableWithJitteredBackoff(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(503), status(503), ok(health)}}
	c, rec := client(t, f)
	if _, err := c.HealthWithResponse(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 3 {
		t.Fatalf("attempts = %d, want 3", len(f.requests))
	}
	// random 0.5 of the ceiling: 0.5 × 500 ms, then 0.5 × 1 s.
	want := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond}
	if len(rec.sleeps) != 2 || rec.sleeps[0] != want[0] || rec.sleeps[1] != want[1] {
		t.Fatalf("sleeps = %v, want %v", rec.sleeps, want)
	}
}

func TestBackoffNeverExceedsCap(t *testing.T) {
	tr := &transport{cfg: &config{backoffBase: 500 * time.Millisecond, backoffCap: 30 * time.Second, random: func() float64 { return 0.999 }, now: time.Now}}
	for attempt := 0; attempt < 20; attempt++ {
		if d := tr.backoff(attempt, nil); d < 0 || d > 30*time.Second {
			t.Fatalf("attempt %d: %v outside [0, 30s]", attempt, d)
		}
	}
}

func TestRetryAfterSecondsWins(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(429, "Retry-After", "2"), ok(health)}}
	c, rec := client(t, f)
	if _, err := c.HealthWithResponse(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.sleeps) != 1 || rec.sleeps[0] != 2*time.Second {
		t.Fatalf("sleeps = %v, want [2s]", rec.sleeps)
	}
}

func TestRetryAfterDate(t *testing.T) {
	at := epoch.Add(7 * time.Second).Format(http.TimeFormat)
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(429, "Retry-After", at), ok(health)}}
	c, rec := client(t, f)
	if _, err := c.HealthWithResponse(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.sleeps[0] != 7*time.Second {
		t.Fatalf("sleep = %v, want 7s", rec.sleeps[0])
	}
}

func TestRateLimitResetWhenNoRetryAfter(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(429, "RateLimit-Reset", "3"), ok(health)}}
	c, rec := client(t, f)
	if _, err := c.HealthWithResponse(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.sleeps[0] != 3*time.Second {
		t.Fatalf("sleep = %v, want 3s", rec.sleeps[0])
	}
}

func TestHintIsCapped(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(429, "Retry-After", "600"), ok(health)}}
	c, rec := client(t, f)
	if _, err := c.HealthWithResponse(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.sleeps[0] != 30*time.Second {
		t.Fatalf("sleep = %v, want the 30s cap", rec.sleeps[0])
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(429)}}
	c, _ := client(t, f, WithMaxRetries(2))
	_, err := c.HealthWithResponse(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 429 || apiErr.Attempts != 3 {
		t.Fatalf("err = %#v", err)
	}
	if len(f.requests) != 3 {
		t.Fatalf("attempts = %d, want 3", len(f.requests))
	}
}

func TestRetryTable(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		call    func(*Client) error
		retried bool
	}{
		{"400 read", 400, func(c *Client) error { _, err := c.HealthWithResponse(context.Background()); return err }, false},
		{"404 read", 404, func(c *Client) error { _, err := c.HealthWithResponse(context.Background()); return err }, false},
		{"500 read", 500, func(c *Client) error { _, err := c.HealthWithResponse(context.Background()); return err }, true},
		{"500 keyed write", 500, func(c *Client) error {
			_, err := c.ClaimWithResponse(context.Background(), "w", nil, api.ClaimRequest{Who: Who("a"), Slots: SlotList(9)})
			return err
		}, true},
		{"500 explainClaim", 500, func(c *Client) error {
			_, err := c.ExplainClaimWithResponse(context.Background(), "w", api.ClaimRequest{Who: Who("a"), Slots: SlotList(9)})
			return err
		}, true},
		{"500 unkeyed write", 500, func(c *Client) error {
			_, err := c.CreateKeyWithResponse(context.Background(), api.CreateKeyJSONRequestBody{})
			return err
		}, false},
		{"503 unkeyed write", 503, func(c *Client) error {
			_, err := c.CreateKeyWithResponse(context.Background(), api.CreateKeyJSONRequestBody{})
			return err
		}, true},
		{"429 unkeyed write", 429, func(c *Client) error {
			_, err := c.CreateKeyWithResponse(context.Background(), api.CreateKeyJSONRequestBody{})
			return err
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(tc.status)}}
			c, _ := client(t, f, WithMaxRetries(1))
			if err := tc.call(c); err == nil {
				t.Fatal("want an error")
			}
			if got := len(f.requests) == 2; got != tc.retried {
				t.Fatalf("attempts = %d, retried want %v", len(f.requests), tc.retried)
			}
		})
	}
}

const receipt = `{"wrote":"0x200","slots":[9],"displaced":[],"commitSeq":3,"head":"0123456789abcdef0123456789abcdef"}`

func TestIdempotencyKeyReusedAcrossRetries(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(503), status(502), created(receipt)}}
	c, _ := client(t, f)
	if _, err := c.ClaimWithResponse(context.Background(), "w", nil, api.ClaimRequest{Who: Who("a"), Slots: SlotList(9)}); err != nil {
		t.Fatal(err)
	}
	k := f.requests[0].Header.Get("Idempotency-Key")
	if len(k) != 36 {
		t.Fatalf("key = %q, want a UUID", k)
	}
	for i, r := range f.requests {
		if r.Header.Get("Idempotency-Key") != k {
			t.Fatalf("attempt %d sent %q, want %q", i, r.Header.Get("Idempotency-Key"), k)
		}
		if f.bodies[i] != f.bodies[0] || f.bodies[i] == "" {
			t.Fatalf("attempt %d body %q differs from %q", i, f.bodies[i], f.bodies[0])
		}
	}
}

func TestIdempotencyKeyOnlyWhereDeclared(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){ok(health), created(`{}`)}}
	c, _ := client(t, f)
	_, _ = c.HealthWithResponse(context.Background())
	_, _ = c.CreateKeyWithResponse(context.Background(), api.CreateKeyJSONRequestBody{})
	for _, r := range f.requests {
		if r.Header.Get("Idempotency-Key") != "" {
			t.Fatalf("%s %s carried a key", r.Method, r.URL.Path)
		}
	}
}

func TestCallersIdempotencyKey(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){created(receipt)}}
	c, _ := client(t, f)
	ctx := WithIdempotencyKey(context.Background(), "order-42")
	if _, err := c.ClaimWithResponse(ctx, "w", nil, api.ClaimRequest{Who: Who("a"), Slots: SlotList(9)}); err != nil {
		t.Fatal(err)
	}
	if got := f.requests[0].Header.Get("Idempotency-Key"); got != "order-42" {
		t.Fatalf("key = %q", got)
	}
}

func TestHeaders(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){ok(health)}}
	c, _ := client(t, f)
	_, _ = c.HealthWithResponse(context.Background())
	r := f.requests[0]
	if r.Header.Get("Authorization") != "Bearer sbk_testsecret123" {
		t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
	}
	if r.Header.Get("User-Agent") != "schedule-bit-go/"+Version {
		t.Fatalf("user-agent = %q", r.Header.Get("User-Agent"))
	}
}

func TestKeyFromEnvironment(t *testing.T) {
	t.Setenv("SCHEDULE_BIT_API_KEY", "sbk_fromenv")
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.key != "sbk_fromenv" || c.BaseURL() != DefaultBaseURL {
		t.Fatalf("key %q base %q", c.cfg.key, c.BaseURL())
	}
}

func TestAttemptTimeoutIsRetriedThenSurfaced(t *testing.T) {
	slow := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){slow}}
	c, _ := client(t, f, WithTimeout(50*time.Millisecond), WithMaxRetries(1))
	_, err := c.HealthWithResponse(context.Background())
	var netErr *NetworkError
	if !errors.As(err, &netErr) || netErr.Attempts != 2 {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cause = %v, want the attempt's deadline", err)
	}
}

func TestCancellationStopsWaiting(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){status(503)}}
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(WithBaseURL(httptest.NewServer(f).URL), WithSleep(func(c context.Context, d time.Duration) error {
		cancel()
		return defaultSleep(c, d)
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.HealthWithResponse(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want canceled", err)
	}
	if len(f.requests) != 1 {
		t.Fatalf("attempts = %d, want 1", len(f.requests))
	}
}

func TestRefusalIsTyped(t *testing.T) {
	refused := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(409)
		_, _ = io.WriteString(w, `{"type":"https://schedule-bit.dev/problems/refused","title":"Refused","status":409,"detail":"slot 9 is held by bob","refusal":{"by":"held","conflictsWith":["bob"],"slots":"0x200","outranked":true,"pinned":true}}`)
	}
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){refused}}
	c, _ := client(t, f)
	_, err := c.ClaimWithResponse(context.Background(), "w", nil, api.ClaimRequest{Who: Who("a"), Slots: SlotList(9)})
	var refusal *Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %T %v", err, err)
	}
	if refusal.Reason.By != api.RefusalByHeld || (*refusal.Reason.ConflictsWith)[0] != "bob" || *refusal.Reason.Slots != "0x200" {
		t.Fatalf("reason = %+v", refusal.Reason)
	}
	var outranked bool
	if !refusal.Member("outranked", &outranked) || !outranked {
		t.Fatal("outranked member missing")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 409 {
		t.Fatal("a refusal is an APIError too")
	}
	if len(f.requests) != 1 {
		t.Fatal("a refusal is never retried")
	}
	receipt, r, err := c.TryClaim(context.Background(), "w", api.ClaimRequest{Who: Who("a"), Slots: SlotList(9)})
	if receipt != nil || r == nil || err != nil {
		t.Fatalf("TryClaim = %v %v %v", receipt, r, err)
	}
}

func TestLocalTimeError(t *testing.T) {
	fold := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(422)
		_, _ = io.WriteString(w, `{"type":"https://schedule-bit.dev/problems/ambiguous-local-time","title":"Ambiguous local time","status":422,"detail":"2026-10-25T01:00 happens twice in Europe/London","zone":"Europe/London","kind":"repeated","local":["2026-10-25T01:00"],"candidates":[{"occurrence":"earlier","instant":"2026-10-25T00:00:00Z","date":"2026-10-25","slot":0},{"occurrence":"later","instant":"2026-10-25T01:00:00Z","date":"2026-10-25","slot":1}]}`)
	}
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){fold}}
	c, _ := client(t, f)
	_, err := c.ClaimWithResponse(context.Background(), "w", nil, api.ClaimRequest{Who: Who("a"), Slots: SlotList(1)})
	var lt *LocalTimeError
	if !errors.As(err, &lt) || lt.Kind != "repeated" || len(lt.Candidates) != 2 || lt.Candidates[1].Slot != 1 {
		t.Fatalf("err = %#v", err)
	}
	chosen, err := lt.Choose(lt.Candidates[1], Local{Zone: "Europe/London", At: "2026-10-25"})
	if err != nil || chosen.Occurrence != "later" {
		t.Fatalf("chosen = %+v %v", chosen, err)
	}
}

func TestErrorsRedactKeys(t *testing.T) {
	leaky := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(400)
		_, _ = io.WriteString(w, `{"type":"x","title":"Bad","status":400,"detail":"key sbk_abcDEF123 and sk-ant-api03-xyz and whsec_q1 rejected"}`)
	}
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){leaky}}
	c, _ := client(t, f)
	_, err := c.HealthWithResponse(context.Background())
	var apiErr *APIError
	errors.As(err, &apiErr)
	for _, s := range []string{err.Error(), string(apiErr.Body), apiErr.Detail} {
		if strings.Contains(s, "abcDEF123") || strings.Contains(s, "api03-xyz") || strings.Contains(s, "whsec_q1") {
			t.Fatalf("leaked: %s", s)
		}
	}
	if !strings.Contains(err.Error(), "sbk_…redacted") {
		t.Fatalf("err = %s", err)
	}
}

func TestRedact(t *testing.T) {
	cases := map[string]string{
		"Authorization: Bearer sbk_abc":  "Authorization: Bearer …redacted",
		"operator sbo_zz9 here":          "operator sbo_…redacted here",
		"viewer sbv_t-1":                 "viewer sbv_…redacted",
		"anthropic sk-ant-api03-abc_DEF": "anthropic sk-ant-…redacted",
		"nothing secret":                 "nothing secret",
	}
	for in, want := range cases {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRateLimitHeadersReachTheHook(t *testing.T) {
	f := &fake{answers: []func(http.ResponseWriter, *http.Request){ok(health, "RateLimit-Limit", "300", "RateLimit-Remaining", "299", "RateLimit-Reset", "60")}}
	var got RateLimit
	c, _ := client(t, f, OnRateLimit(func(r RateLimit) { got = r }))
	_, _ = c.HealthWithResponse(context.Background())
	if !got.Present || got.Limit != 300 || got.Remaining != 299 || got.Reset != time.Minute {
		t.Fatalf("rate limit = %+v", got)
	}
}

func TestTokenBucketSpacesRequests(t *testing.T) {
	now := epoch
	b := newBucket(60, func() time.Time { return now }) // one a second
	var waited time.Duration
	sleep := func(_ context.Context, d time.Duration) error { waited += d; now = now.Add(d); return nil }
	for i := 0; i < 61; i++ {
		if err := b.wait(context.Background(), sleep); err != nil {
			t.Fatal(err)
		}
	}
	if waited < 990*time.Millisecond || waited > 1010*time.Millisecond {
		t.Fatalf("61st request waited %v, want ~1s", waited)
	}
}

func TestOperationTable(t *testing.T) {
	cases := map[string]struct{ keyed, repeatable bool }{
		"POST /v1/worlds/w/claims":         {true, true},
		"POST /v1/worlds/acme%2Fw/claims":  {true, true},
		"POST /v1/worlds/w/claims:explain": {false, true},
		"GET /v1/worlds/w/journal":         {false, true},
		"POST /v1/keys":                    {false, false},
		"POST /v1/keys/key_1:rotate":       {false, false},
	}
	for in, want := range cases {
		method, path, _ := strings.Cut(in, " ")
		op, ok := operationOf(method, path)
		if !ok || op.Keyed != want.keyed || op.Repeatable != want.repeatable {
			t.Errorf("%s: %+v (found %v)", in, op, ok)
		}
	}
}
