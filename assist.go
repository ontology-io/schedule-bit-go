package schedulebit

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/ontology-io/schedule-bit-go/api"
)

// AssistEvent is one event of a Director run: Type equals the SSE event name
// ("open", "tool", "text", "done", …) and Data is its JSON.
type AssistEvent struct {
	Type string
	ID   string
	Data json.RawMessage
}

// Decode reads the event's data into v.
func (e AssistEvent) Decode(v any) error { return json.Unmarshal(e.Data, v) }

// AssistStream yields a Director run's events as they arrive.
//
//	s, err := c.Assist(ctx, body)
//	for s.Next() { ev := s.Event(); … }
//	err = s.Err()
type AssistStream struct {
	body   io.ReadCloser
	lines  *bufio.Scanner
	event  AssistEvent
	err    error
	closed bool
}

// Assist starts a Director run (POST /v1/assist) and streams its events. A
// caller's own Anthropic key goes in body.AnthropicKey; it is never logged
// and never appears in an error.
func (c *Client) Assist(ctx context.Context, body api.AssistJSONRequestBody) (*AssistStream, error) {
	req, err := api.NewAssistRequest(c.cfg.base, body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	// A stream lives longer than one attempt's timeout: only ctx bounds it.
	resp, err := c.doer().Do(req.WithContext(WithCallTimeout(ctx, -1)))
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 4<<20)
	return &AssistStream{body: resp.Body, lines: sc}, nil
}

// Next reads the next event; false at the end of the run or on an error.
func (s *AssistStream) Next() bool {
	if s.closed {
		return false
	}
	var ev AssistEvent
	var data strings.Builder
	for s.lines.Scan() {
		line := s.lines.Text()
		if line == "" {
			if data.Len() > 0 || ev.Type != "" {
				ev.Data = json.RawMessage(data.String())
				s.event = ev
				return true
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			ev.Type = value
		case "id":
			ev.ID = value
		case "data":
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(value)
		}
	}
	s.err = s.lines.Err()
	s.Close()
	return false
}

// Event is the event Next read.
func (s *AssistStream) Event() AssistEvent { return s.event }

// Err is why the stream stopped early; nil at a normal end.
func (s *AssistStream) Err() error { return redactErr(s.err) }

// Close stops reading the run.
func (s *AssistStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.body.Close()
}
