package schedulebit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/ontology-io/schedule-bit-go/api"
)

// Frame is one thing a watch yields, in order.
type Frame struct {
	// Type is the frame's type: "hello", an event type ("commit",
	// "claim.made", …), "presence…", or "journal" for a commit read back from
	// the journal after a reconnect.
	Type string
	// Raw is the frame as the server sent it (for "journal", the commit).
	Raw json.RawMessage
	// Event is set for World events.
	Event *api.WorldEvent
	// Commit is set for "journal" frames.
	Commit *api.JournalCommit
}

// WatchOptions shape a watch.
type WatchOptions struct {
	// Name and Color are what other watchers see, with presence.
	Name, Color string
	// Presence also carries other viewers' presence frames.
	Presence bool
	// Since, when > 0, replays the journal from this sequence before going live.
	Since int
}

// Watch is a live view of one World. It reconnects on its own, with the
// client's back-off; on reconnecting it reads the journal from the last
// sequence it saw, so no commit is missed, and it never yields the same
// event twice. Read Frames until it closes, then Err.
type Watch struct {
	frames chan Frame
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
}

// Frames is the stream; it closes when the watch ends.
func (w *Watch) Frames() <-chan Frame { return w.frames }

// Err is why the watch ended: nil after Close or a cancelled context.
func (w *Watch) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Close ends the watch; it never reconnects after.
func (w *Watch) Close() error {
	w.cancel()
	<-w.done
	return nil
}

// Watch opens a live view of world. It returns once the first connection is
// up (or failed for good); later drops reconnect in the background.
func (c *Client) Watch(ctx context.Context, world string, opts WatchOptions) (*Watch, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &Watch{frames: make(chan Frame, 64), cancel: cancel, done: make(chan struct{})}
	s := &watcher{c: c, world: world, opts: opts, out: w.frames, last: opts.Since, seen: map[string]bool{}}
	conn, err := s.connect(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	go func() {
		defer close(w.done)
		defer close(w.frames)
		err := s.run(ctx, conn)
		if ctx.Err() != nil {
			err = nil
		}
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
	}()
	return w, nil
}

type watcher struct {
	c     *Client
	world string
	opts  WatchOptions
	out   chan<- Frame
	last  int             // highest commit sequence yielded
	seen  map[string]bool // event ids yielded at sequences > last-window
}

func (s *watcher) connect(ctx context.Context) (*websocket.Conn, error) {
	body := api.WatchTicketRequest{}
	if s.opts.Name != "" {
		body.Name = &s.opts.Name
	}
	if s.opts.Color != "" {
		body.Color = &s.opts.Color
	}
	ticket, err := s.c.WatchTicketWithResponse(ctx, s.world, body)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(s.c.cfg.base)
	if err != nil {
		return nil, err
	}
	u.Scheme = map[string]string{"https": "wss", "http": "ws"}[u.Scheme]
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/worlds/" + url.PathEscape(s.world) + "/watch"
	q := url.Values{"ticket": {ticket.JSON201.Ticket}}
	if s.opts.Presence {
		q.Set("presence", "true")
	}
	u.RawQuery = q.Encode()
	conn, resp, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{
		HTTPClient: s.c.cfg.http,
		HTTPHeader: map[string][]string{"User-Agent": {userAgent}},
	})
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		return nil, &NetworkError{Operation: "watch", Attempts: 1, Err: redactErr(fmt.Errorf("dial (status %d): %w", status, err))}
	}
	conn.SetReadLimit(1 << 22)
	return conn, nil
}

func (s *watcher) run(ctx context.Context, conn *websocket.Conn) error {
	if s.last > 0 {
		if err := s.catchUp(ctx); err != nil {
			return err
		}
	}
	for attempt := 0; ; {
		err := s.read(ctx, conn)
		conn.Close(websocket.StatusNormalClosure, "")
		if ctx.Err() != nil {
			return nil
		}
		// A watch reconnects for as long as it is open; WithMaxRetries(0)
		// turns that off, and the attempt count only grows the back-off.
		if s.c.cfg.maxRetries == 0 {
			return err
		}
		t := &transport{cfg: s.c.cfg}
		if e := s.c.cfg.sleep(ctx, t.backoff(attempt, nil)); e != nil {
			return nil
		}
		attempt++
		next, cerr := s.connect(ctx)
		if cerr != nil {
			continue
		}
		conn = next
		if err := s.catchUp(ctx); err != nil {
			continue
		}
		attempt = 0
	}
}

// read yields frames until the socket fails.
func (s *watcher) read(ctx context.Context, conn *websocket.Conn) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var head struct {
			Type      string `json:"type"`
			ID        string `json:"id"`
			CommitSeq *int   `json:"commitSeq"`
		}
		if json.Unmarshal(data, &head) != nil {
			continue
		}
		f := Frame{Type: head.Type, Raw: append(json.RawMessage(nil), data...)}
		if head.CommitSeq != nil {
			key := fmt.Sprintf("%d:%s", *head.CommitSeq, head.Type)
			if s.seen[key] {
				continue
			}
			var ev api.WorldEvent
			if json.Unmarshal(data, &ev) == nil {
				f.Event = &ev
			}
			s.mark(key, *head.CommitSeq)
		}
		if !s.emit(ctx, f) {
			return ctx.Err()
		}
	}
}

// catchUp reads the journal from the last sequence yielded, so a drop never
// loses a commit.
func (s *watcher) catchUp(ctx context.Context) error {
	for {
		since := s.last
		limit := 1000
		j, err := s.c.JournalWithResponse(ctx, s.world, &api.JournalParams{Since: &since, Limit: &limit})
		if err != nil {
			return err
		}
		if j.JSON200 == nil || len(j.JSON200.Commits) == 0 {
			return nil
		}
		for i := range j.JSON200.Commits {
			c := j.JSON200.Commits[i]
			if c.Seq <= s.last {
				continue
			}
			raw, _ := json.Marshal(c)
			s.mark(fmt.Sprintf("%d:commit", c.Seq), c.Seq)
			if !s.emit(ctx, Frame{Type: "journal", Raw: raw, Commit: &c}) {
				return ctx.Err()
			}
		}
		if len(j.JSON200.Commits) < limit {
			return nil
		}
	}
}

const seenWindow = 4096

func (s *watcher) mark(key string, seq int) {
	s.seen[key] = true
	if seq > s.last {
		s.last = seq
	}
	if len(s.seen) > 2*seenWindow {
		for k := range s.seen {
			var n int
			if _, err := fmt.Sscanf(k, "%d:", &n); err == nil && n < s.last-seenWindow {
				delete(s.seen, k)
			}
		}
	}
}

func (s *watcher) emit(ctx context.Context, f Frame) bool {
	select {
	case s.out <- f:
		return true
	case <-ctx.Done():
		return false
	}
}
