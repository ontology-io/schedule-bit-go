package schedulebit

import (
	"fmt"
	"time"

	"github.com/ontology-io/schedule-bit-go/api"
)

// Local is a position named in someone's own time: a zone, a local date (or
// date and hour) with no offset, and, at a clock change, which of a repeated
// or skipped local time is meant. The API resolves it to UTC positions; a
// local day may span two UTC days, and is still written as one commit.
type Local struct {
	Zone       string
	At         string
	Occurrence string // "", "earlier" or "later"
}

// LocalDay is t's wall-clock date in t's location: the position of a whole
// local day on an hour World (slots are local hours 0–23).
func LocalDay(t time.Time) Local {
	return Local{Zone: zoneName(t), At: t.Format("2006-01-02")}
}

// LocalHour is t's wall-clock date and hour in t's location: the position of
// one local hour on a minute World (slots are minutes 0–59).
func LocalHour(t time.Time) Local {
	return Local{Zone: zoneName(t), At: t.Format("2006-01-02T15")}
}

// Earlier picks the first of a repeated local time (the clocks went back),
// or reads a skipped one with the offset before the change.
func (l Local) Earlier() Local { l.Occurrence = "earlier"; return l }

// Later picks the second of a repeated local time, or reads a skipped one
// with the offset after the change.
func (l Local) Later() Local { l.Occurrence = "later"; return l }

func zoneName(t time.Time) string {
	if n := t.Location().String(); n != "Local" {
		return n
	}
	return "UTC"
}

// Fields are the request members: zone, at and, if chosen, occurrence. Set
// them on a request body, e.g. body.Zone, body.At, body.Occurrence = l.Fields().
func (l Local) Fields() (*api.ZoneName, *api.LocalAt, *api.Occurrence) {
	z, a := l.Zone, l.At
	var o *api.Occurrence
	if l.Occurrence != "" {
		v := api.Occurrence(l.Occurrence)
		o = &v
	}
	return &z, &a, o
}

// Claim is a claim request at this local position.
func (l Local) Claim(who api.OccupierRef, slots api.SlotsInput) api.ClaimRequest {
	body := api.ClaimRequest{Who: who, Slots: slots}
	body.Zone, body.At, body.Occurrence = l.Fields()
	return body
}

// Choose is the position the candidate names: resend with it after a
// LocalTimeError.
func (e *LocalTimeError) Choose(c LocalCandidate, at Local) (Local, error) {
	switch c.Occurrence {
	case "earlier":
		return at.Earlier(), nil
	case "later":
		return at.Later(), nil
	}
	return at, fmt.Errorf("schedulebit: unknown occurrence %q", c.Occurrence)
}
