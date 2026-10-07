package schedulebit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/ontology-io/schedule-bit-go/api"
)

// APIError is an answer the API gave as application/problem+json (RFC 9457),
// or any other failed answer. Refusal and LocalTimeError unwrap to it, so
// errors.As(err, &apiErr) catches all three.
type APIError struct {
	Operation string // the operationId, e.g. "claim"
	Status    int
	Type      string
	Title     string
	Detail    string
	Attempts  int    // how many attempts were made
	Body      []byte // the raw answer, keys redacted
	// RateLimit is the answer's RateLimit-* headers, when present.
	RateLimit RateLimit
}

func (e *APIError) Error() string {
	msg := e.Detail
	if msg == "" {
		msg = e.Title
	}
	if msg == "" {
		msg = http.StatusText(e.Status)
	}
	return fmt.Sprintf("schedulebit: %s: %d %s", e.Operation, e.Status, msg)
}

// Refusal is the engine saying no (409): the door judged the claim and did
// not admit it. Reason.By names the guard (held, rule, rest, horizon,
// capacity, …); Members holds the reason's other members by name, raw, since
// they depend on By (e.g. "outranked", "pinned").
type Refusal struct {
	APIError
	Reason  api.Refusal
	Members map[string]json.RawMessage
}

func (e *Refusal) Error() string {
	return fmt.Sprintf("schedulebit: %s refused (%s): %s", e.Operation, e.Reason.By, e.Detail)
}

// Unwrap lets errors.As find the APIError inside.
func (e *Refusal) Unwrap() error { return &e.APIError }

// Member decodes one of the reason's members into v; false when absent.
func (e *Refusal) Member(name string, v any) bool {
	raw, ok := e.Members[name]
	return ok && json.Unmarshal(raw, v) == nil
}

// LocalTimeError is a 422 for a local time the zone repeats or skips
// (ambiguous-local-time), or one that doesn't fall on a slot
// (misaligned-local-time). Resend with an Occurrence from Candidates.
type LocalTimeError struct {
	APIError
	Zone       string
	Kind       string // "repeated" or "skipped"
	Local      []string
	Candidates []LocalCandidate
}

// LocalCandidate is one reading of an ambiguous local time.
type LocalCandidate struct {
	Occurrence string `json:"occurrence"`
	Instant    string `json:"instant"`
	Date       string `json:"date"`
	Hour       *int   `json:"hour,omitempty"`
	Slot       int    `json:"slot"`
}

func (e *LocalTimeError) Error() string {
	return fmt.Sprintf("schedulebit: %s: %s", e.Operation, e.Detail)
}

// Unwrap lets errors.As find the APIError inside.
func (e *LocalTimeError) Unwrap() error { return &e.APIError }

// NetworkError is a request that never got an answer, after every retry. Err
// is the last cause.
type NetworkError struct {
	Operation string
	Attempts  int
	Err       error
}

func (e *NetworkError) Error() string {
	return fmt.Sprintf("schedulebit: %s: no answer after %d attempt(s): %v", e.Operation, e.Attempts, e.Err)
}

func (e *NetworkError) Unwrap() error { return e.Err }

// errorOf turns a failed answer into the most specific error.
func errorOf(op string, resp *http.Response, body []byte, attempts int) error {
	base := APIError{
		Operation: op,
		Status:    resp.StatusCode,
		Attempts:  attempts,
		Body:      []byte(Redact(string(body))),
		RateLimit: parseRateLimit(resp.Header),
	}
	var problem struct {
		Type, Title, Detail string
		Refusal             *json.RawMessage `json:"refusal"`
		Zone                string           `json:"zone"`
		Kind                string           `json:"kind"`
		Local               []string         `json:"local"`
		Candidates          []LocalCandidate `json:"candidates"`
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "json") && json.Unmarshal(body, &problem) == nil {
		base.Type, base.Title, base.Detail = problem.Type, problem.Title, Redact(problem.Detail)
	}
	switch {
	case resp.StatusCode == http.StatusConflict && problem.Refusal != nil:
		r := &Refusal{APIError: base}
		_ = json.Unmarshal(*problem.Refusal, &r.Reason)
		_ = json.Unmarshal(*problem.Refusal, &r.Members)
		delete(r.Members, "by")
		return r
	case resp.StatusCode == http.StatusUnprocessableEntity && problem.Zone != "":
		return &LocalTimeError{APIError: base, Zone: problem.Zone, Kind: problem.Kind, Local: problem.Local, Candidates: problem.Candidates}
	}
	return &base
}
