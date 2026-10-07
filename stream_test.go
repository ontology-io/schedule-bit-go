package schedulebit

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ontology-io/schedule-bit-go/api"
)

func event(seq int, typ string) string {
	return fmt.Sprintf(`{"id":"acme/w:%d:%s","type":%q,"world":"acme/w","at":1,"commitSeq":%d,"head":"0123456789abcdef0123456789abcdef","data":{}}`, seq, typ, typ, seq)
}

func commit(seq int) string {
	return fmt.Sprintf(`{"seq":%d,"tick":1,"actor":null,"kind":"claim","ref":null,"prevHash":"0123456789abcdef0123456789abcdef","hash":"0123456789abcdef0123456789abcdef","entries":[]}`, seq)
}

// A watch that drops reconnects, reads the journal from where it was, and
// never yields an event twice.
func TestWatchReconnectsAndCatchesUp(t *testing.T) {
	var connections, tickets atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/worlds/{w}/watch-tickets", func(w http.ResponseWriter, _ *http.Request) {
		tickets.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = io.WriteString(w, `{"ticket":"wt_abc","expiresAt":1}`)
	})
	mux.HandleFunc("GET /v1/worlds/{w}/journal", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("since") != "1" {
			t.Errorf("journal since = %q, want 1", r.URL.Query().Get("since"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"head":"0123456789abcdef0123456789abcdef","seq":2,"commits":[`+commit(1)+`,`+commit(2)+`]}`)
	})
	mux.HandleFunc("GET /v1/worlds/{w}/watch", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("ticket") != "wt_abc" {
			t.Errorf("ticket = %q", r.URL.Query().Get("ticket"))
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := r.Context()
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","world":{}}`))
		switch connections.Add(1) {
		case 1: // seq 1, then the line drops
			_ = conn.Write(ctx, websocket.MessageText, []byte(event(1, "claim.made")))
			_ = conn.Write(ctx, websocket.MessageText, []byte(event(1, "commit")))
			conn.Close(websocket.StatusGoingAway, "restart")
		default: // seq 2 again (already read from the journal), then seq 3
			_ = conn.Write(ctx, websocket.MessageText, []byte(event(2, "commit")))
			_ = conn.Write(ctx, websocket.MessageText, []byte(event(3, "commit")))
			<-ctx.Done()
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c, err := New(WithBaseURL(srv.URL), WithAPIKey("sbk_x"), WithSleep(func(ctx context.Context, d time.Duration) error { return ctx.Err() }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := c.Watch(ctx, "acme/w", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for f := range w.Frames() {
		seq := ""
		if f.Event != nil && f.Event.CommitSeq != nil {
			seq = fmt.Sprint(*f.Event.CommitSeq)
		}
		if f.Commit != nil {
			seq = fmt.Sprint(f.Commit.Seq)
		}
		got = append(got, f.Type+seq)
		if f.Type == "commit" && seq == "3" {
			break
		}
	}
	_ = w.Close()
	want := "hello claim.made1 commit1 journal2 hello commit3"
	if strings.Join(got, " ") != want {
		t.Fatalf("frames = %q\nwant     %q", strings.Join(got, " "), want)
	}
	if tickets.Load() != 2 {
		t.Fatalf("tickets = %d, want one per connection", tickets.Load())
	}
	if w.Err() != nil {
		t.Fatalf("err after Close = %v", w.Err())
	}
}

func TestAssistStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Errorf("accept = %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": keep-alive\n\nevent: open\nid: 1\ndata: {\"type\":\"open\",\"assistId\":\"as_0123456789abcdef\"}\n\nevent: text\ndata: {\"type\":\"text\",\ndata: \"text\":\"hi\"}\n\nevent: done\ndata: {\"type\":\"done\"}\n\n")
	}))
	defer srv.Close()
	c, _ := New(WithBaseURL(srv.URL))
	s, err := c.Assist(context.Background(), api.AssistRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for s.Next() {
		ev := s.Event()
		var body map[string]any
		if err := ev.Decode(&body); err != nil {
			t.Fatalf("%s: %v (%s)", ev.Type, err, ev.Data)
		}
		types = append(types, ev.Type)
	}
	if s.Err() != nil || strings.Join(types, ",") != "open,text,done" {
		t.Fatalf("types = %v err = %v", types, s.Err())
	}
}

// The checked-in generated code matches the contract (monorepo only: the
// public mirror has no copy of the contract to compare with).
func TestGeneratedCodeIsCurrent(t *testing.T) {
	if _, err := os.Stat("../spec/openapi.json"); err != nil {
		t.Skip("no contract here (the public mirror)")
	}
	if testing.Short() {
		t.Skip("-short")
	}
	out, err := exec.Command("go", "run", "./internal/cmd/gen", "-check").CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}
