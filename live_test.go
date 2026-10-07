package schedulebit

// The live conformance suite (sdks/POLICY.md): the same scenarios every SDK
// runs against the deployed dev API, when SCHEDULE_BIT_LIVE=1. Each run mints
// its own key: with the operator key (env SCHEDULE_BIT_OPERATOR_KEY, or the
// file named by SCHEDULE_BIT_OPERATOR_KEY_FILE) into workspace
// sdk-go-<run id>, else from one dev sandbox. That workspace is the run's
// cleanup boundary.
//
//	SCHEDULE_BIT_LIVE=1 go test -run Live -v .

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ontology-io/schedule-bit-go/api"
)

const devBase = "https://schedule-bit-api-dev.khaled-mailhub.workers.dev"

func liveBase() string {
	if b := os.Getenv("SCHEDULE_BIT_LIVE_BASE"); b != "" {
		return b
	}
	return devBase
}

func live(t *testing.T) {
	if os.Getenv("SCHEDULE_BIT_LIVE") != "1" {
		t.Skip("set SCHEDULE_BIT_LIVE=1 to run against the deployed API")
	}
}

// operatorKey is the dev operator key, if one is at hand.
func operatorKey(t *testing.T) string {
	if k := strings.TrimSpace(os.Getenv("SCHEDULE_BIT_OPERATOR_KEY")); k != "" {
		return k
	}
	if f := os.Getenv("SCHEDULE_BIT_OPERATOR_KEY_FILE"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading the operator key file: %v", err)
		}
		return strings.TrimSpace(string(b))
	}
	return ""
}

func runID() string {
	if id := os.Getenv("GITHUB_RUN_ID"); id != "" {
		return id
	}
	return strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))[:12]
}

type minted struct {
	key, workspace string
	sandbox        bool // the key came from POST /v1/sandbox (scenario 10 is then done)
}

// mint makes this run's workspace and key: with the operator key when there
// is one (CI), else with one dev sandbox (no key and no human check on dev;
// the API allows 5 an hour per address, so one per run).
func mint(t *testing.T, ctx context.Context) minted {
	open, err := New(WithBaseURL(liveBase()), WithAPIKey(""), WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	if op := operatorKey(t); op != "" {
		ws := "sdk-go-" + runID()
		label, plan := "sdk live", api.OperatorIssueKeyJSONBodyPlan("beta")
		resp, err := open.OperatorIssueKeyWithResponse(ctx, api.OperatorIssueKeyJSONRequestBody{Workspace: ws, Label: &label, Plan: &plan},
			func(_ context.Context, r *http.Request) error { r.Header.Set("Authorization", "Bearer "+op); return nil })
		if err != nil {
			t.Fatalf("minting a key: %v", err)
		}
		return minted{key: resp.JSON201.Key.Secret, workspace: ws}
	}
	s, err := open.CreateSandboxWithResponse(ctx, api.SandboxRequest{})
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == 429 {
		t.Skip("no operator key, and this address made its 5 sandboxes this hour")
	}
	if err != nil {
		t.Fatalf("no operator key, and the sandbox failed: %v", err)
	}
	return minted{key: s.JSON201.Key.Secret, workspace: s.JSON201.Workspace, sandbox: true}
}

func date(s string) *openapi_types.Date {
	d, _ := time.Parse("2006-01-02", s)
	return &openapi_types.Date{Time: d}
}

func TestLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	live(t)
	m := mint(t, ctx)
	key, workspace := m.key, m.workspace
	var mu sync.Mutex
	var limits []RateLimit
	c, err := New(WithBaseURL(liveBase()), WithAPIKey(key), OnRateLimit(func(r RateLimit) {
		mu.Lock()
		limits = append(limits, r)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatal(err)
	}
	world := "conformance"
	// About five weeks ahead, so no slot is in the past.
	ahead := time.Now().UTC().AddDate(0, 0, 35)
	day := ahead.Format("2006-01-02")

	t.Run("1 health, whoami, presets", func(t *testing.T) {
		h, err := c.HealthWithResponse(ctx)
		if err != nil || h.JSON200 == nil {
			t.Fatalf("health: %v", err)
		}
		me, err := c.WhoamiWithResponse(ctx)
		if err != nil || me.JSON200 == nil || me.JSON200.Workspace != workspace {
			t.Fatalf("whoami: %v %+v", err, me)
		}
		p, err := c.PresetsWithResponse(ctx)
		if err != nil || p.JSON200 == nil {
			t.Fatalf("presets: %v", err)
		}
	})

	t.Run("2 world, occupiers, claim, refusal, explain, release", func(t *testing.T) {
		preset := api.CreateWorldPreset("temporal-hour")
		if _, err := c.CreateWorldWithResponse(ctx, nil, api.CreateWorld{Name: world, Preset: &preset}); err != nil {
			t.Fatalf("createWorld: %v", err)
		}
		ten, yes := 10, true
		for _, o := range []api.DeclareOccupier{{Name: "boss", Rank: &ten, Pinned: &yes}, {Name: "alice"}} {
			if _, err := c.DeclareOccupierWithResponse(ctx, world, nil, o); err != nil {
				t.Fatalf("declare %s: %v", o.Name, err)
			}
		}
		r, err := c.ClaimWithResponse(ctx, world, nil, api.ClaimRequest{Who: Who("boss"), Slots: SlotRange(9, 10).SlotsInput(), Date: date(day)})
		if err != nil || r.HTTPResponse.StatusCode != 201 {
			t.Fatalf("claim: %v", err)
		}
		if !MustMask(r.JSON201.Wrote).Equal(MaskOf(9, 10)) {
			t.Fatalf("wrote %s", r.JSON201.Wrote)
		}
		receipt, refusal, err := c.TryClaim(ctx, world, api.ClaimRequest{Who: Who("alice"), Slots: SlotList(9), Date: date(day)})
		if err != nil || receipt != nil || refusal == nil {
			t.Fatalf("a claim over boss: %v %v %v", receipt, refusal, err)
		}
		if refusal.Reason.By != api.RefusalByHeld || refusal.Status != 409 {
			t.Fatalf("refusal: %+v", refusal.Reason)
		}
		// explainClaim judges without writing: the same refusal, typed, and
		// nothing changed.
		_, err = c.ExplainClaimWithResponse(ctx, world, api.ClaimRequest{Who: Who("alice"), Slots: SlotList(9), Date: date(day)})
		var why *Refusal
		if !errors.As(err, &why) || why.Reason.By != api.RefusalByHeld {
			t.Fatalf("explainClaim: %v", err)
		}
		if x, err := c.ExplainClaimWithResponse(ctx, world, api.ClaimRequest{Who: Who("alice"), Slots: SlotList(11), Date: date(day)}); err != nil || x.JSON200 == nil {
			t.Fatalf("explainClaim of a free slot: %v", err)
		}
		rel, err := c.ReleaseWithResponse(ctx, world, nil, api.ReleaseJSONRequestBody{Who: Who("boss"), Slots: SlotList(10), Date: date(day)})
		if err != nil || rel.JSON200 == nil {
			t.Fatalf("release: %v", err)
		}
	})

	t.Run("3 offered and planes, a mask round trip", func(t *testing.T) {
		o, err := c.OfferedWithResponse(ctx, world, &api.OfferedParams{Date: date(day)})
		if err != nil || o.JSON200 == nil {
			t.Fatalf("offered: %v", err)
		}
		offered := MustMask(o.JSON200.Offered)
		if offered.Has(9) || !offered.Has(10) || !offered.Has(11) {
			t.Fatalf("offered %s: 9 held by boss, 10 released", offered)
		}
		p, err := c.PlanesWithResponse(ctx, world, &api.PlanesParams{Date: date(day)})
		if err != nil || p.JSON200 == nil || len(p.JSON200.Positions) == 0 {
			t.Fatalf("planes: %v", err)
		}
		held := Mask{}
		for _, pl := range p.JSON200.Positions[0].Planes {
			if pl.Role == "held" {
				held = held.Union(MustMask(pl.Mask))
			}
		}
		if !held.Equal(MaskOf(9)) || MustMask(held.Hex()).Hex() != "0x200" {
			t.Fatalf("planes held %s, want 0x200", held)
		}
	})

	t.Run("4 local time: Cairo claim, London fold", func(t *testing.T) {
		cairo, err := time.LoadLocation("Africa/Cairo")
		if err != nil {
			t.Skip("no tz database")
		}
		next := ahead.AddDate(0, 0, 1)
		local := time.Date(next.Year(), next.Month(), next.Day(), 9, 0, 0, 0, cairo)
		r, err := c.ClaimWithResponse(ctx, world, nil, LocalDay(local).Claim(Who("alice"), SlotList(9)))
		if err != nil || r.JSON201 == nil {
			t.Fatalf("Cairo claim: %v", err)
		}
		// 09:00 in Cairo lands on the UTC day and hour the tz database says.
		utc := local.UTC()
		res := r.JSON201.Resolved
		if res == nil || len(*res) != 1 || (*res)[0].Date.Format("2006-01-02") != utc.Format("2006-01-02") || !slices.Equal((*res)[0].Slots, []int{utc.Hour()}) {
			t.Fatalf("resolved: %+v, want %s hour %d", res, utc.Format("2006-01-02"), utc.Hour())
		}
		fold := Local{Zone: "Europe/London", At: "2026-10-25"}
		_, err = c.ClaimWithResponse(ctx, world, nil, fold.Claim(Who("alice"), SlotList(1)))
		var lt *LocalTimeError
		var past *Refusal
		if errors.As(err, &past) && past.Reason.By == api.RefusalByPast {
			return // the fold has gone by: the door refuses the past first
		}
		if !errors.As(err, &lt) || lt.Status != 422 || len(lt.Candidates) != 2 {
			t.Fatalf("London fold: %v", err)
		}
		later, _ := lt.Choose(lt.Candidates[1], fold)
		if _, err := c.ClaimWithResponse(ctx, world, nil, later.Claim(Who("alice"), SlotList(1))); err != nil {
			t.Fatalf("the later 01:00: %v", err)
		}
	})

	t.Run("5 journal since", func(t *testing.T) {
		since := 2
		j, err := c.JournalWithResponse(ctx, world, &api.JournalParams{Since: &since})
		if err != nil || j.JSON200 == nil || len(j.JSON200.Commits) == 0 {
			t.Fatalf("journal: %v", err)
		}
		for _, cm := range j.JSON200.Commits {
			if cm.Seq <= since {
				t.Fatalf("since=%d returned seq %d", since, cm.Seq)
			}
		}
	})

	t.Run("6 watch sees our own claim", func(t *testing.T) {
		w, err := c.Watch(ctx, world, WatchOptions{})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer w.Close()
		r, err := c.ClaimWithResponse(ctx, world, nil, api.ClaimRequest{Who: Who("alice"), Slots: SlotList(15), Date: date(day)})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		deadline := time.After(20 * time.Second)
		for {
			select {
			case f, open := <-w.Frames():
				if !open {
					t.Fatalf("watch ended: %v", w.Err())
				}
				if f.Event != nil && f.Event.CommitSeq != nil && *f.Event.CommitSeq == r.JSON201.CommitSeq {
					return
				}
			case <-deadline:
				t.Fatal("no frame for our claim in 20 s")
			}
		}
	})

	t.Run("7 idempotent retry answers the same", func(t *testing.T) {
		k := "sdk-go-" + runID() + "-retry"
		body := api.ClaimRequest{Who: Who("alice"), Slots: SlotList(16), Date: date(day)}
		a, err := c.ClaimWithResponse(WithIdempotencyKey(ctx, k), world, nil, body)
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		b, err := c.ClaimWithResponse(WithIdempotencyKey(ctx, k), world, nil, body)
		if err != nil {
			t.Fatalf("again: %v", err)
		}
		if a.JSON201.CommitSeq != b.JSON201.CommitSeq || a.JSON201.Head != b.JSON201.Head {
			t.Fatalf("the same key answered differently: %d vs %d", a.JSON201.CommitSeq, b.JSON201.CommitSeq)
		}
	})

	t.Run("8 rate-limit headers parsed", func(t *testing.T) {
		mu.Lock()
		defer mu.Unlock()
		i := slices.IndexFunc(limits, func(r RateLimit) bool { return r.Present && r.Limit > 0 })
		if i < 0 {
			t.Fatalf("no RateLimit-* headers in %d answers", len(limits))
		}
		if l := limits[i]; l.Remaining > l.Limit || l.Reset < 0 {
			t.Fatalf("rate limit %+v", l)
		}
	})

	t.Run("10 sandbox mints a working key", func(t *testing.T) {
		if m.sandbox {
			return // the run's own key is a sandbox's: every scenario above used it
		}
		if os.Getenv("SCHEDULE_BIT_LIVE_SANDBOX") == "0" {
			t.Skip("SCHEDULE_BIT_LIVE_SANDBOX=0")
		}
		open, _ := New(WithBaseURL(liveBase()), WithAPIKey(""), WithMaxRetries(0))
		s, err := open.CreateSandboxWithResponse(ctx, api.SandboxRequest{})
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status == 429 {
			t.Skip("this address made 5 sandboxes this hour")
		}
		if err != nil || s.JSON201 == nil {
			t.Fatalf("sandbox: %v", err)
		}
		sb, _ := New(WithBaseURL(liveBase()), WithAPIKey(s.JSON201.Key.Secret))
		ws, err := sb.ListWorldsWithResponse(ctx)
		if err != nil || ws.JSON200 == nil {
			t.Fatalf("the sandbox key reads: %v", err)
		}
	})
}
