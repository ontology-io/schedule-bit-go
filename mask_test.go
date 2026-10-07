package schedulebit

import (
	"slices"
	"testing"
	"time"
)

func TestMaskRoundTrip(t *testing.T) {
	m := MustMask("0x600")
	if !slices.Equal(m.Slots(), []int{9, 10}) || m.Hex() != "0x600" || m.Count() != 2 {
		t.Fatalf("0x600 → %v %s %d", m.Slots(), m.Hex(), m.Count())
	}
	if !MaskOf(9, 10).Equal(m) {
		t.Fatal("MaskOf(9,10) != 0x600")
	}
	if e, _ := ParseMask("0x0"); !e.Empty() || e.Hex() != "0x0" {
		t.Fatal("0x0 is empty")
	}
	if _, err := ParseMask("0xzz"); err == nil {
		t.Fatal("0xzz parsed")
	}
}

func TestMaskKeepsHighBits(t *testing.T) {
	m := MaskOf(0, 53, 59, 200)
	back := MustMask(m.Hex())
	if !slices.Equal(back.Slots(), []int{0, 53, 59, 200}) {
		t.Fatalf("lost bits: %v", back.Slots())
	}
}

func TestMaskSetOperations(t *testing.T) {
	a, b := SlotRange(9, 12), MaskOf(11, 12, 13)
	if !slices.Equal(a.Union(b).Slots(), []int{9, 10, 11, 12, 13}) ||
		!slices.Equal(a.Intersect(b).Slots(), []int{11, 12}) ||
		!slices.Equal(a.Without(b).Slots(), []int{9, 10}) {
		t.Fatal("set operations")
	}
	if !a.Has(9) || a.Has(13) || a.Has(-1) {
		t.Fatal("Has")
	}
	if a.Hex() != "0x1e00" {
		t.Fatalf("a = %s", a.Hex())
	}
}

func TestLocal(t *testing.T) {
	cairo, err := time.LoadLocation("Africa/Cairo")
	if err != nil {
		t.Skip("no tz database")
	}
	at := time.Date(2026, 5, 20, 9, 0, 0, 0, cairo)
	if d := LocalDay(at); d.Zone != "Africa/Cairo" || d.At != "2026-05-20" {
		t.Fatalf("day = %+v", d)
	}
	h := LocalHour(at).Later()
	if h.At != "2026-05-20T09" || h.Occurrence != "later" {
		t.Fatalf("hour = %+v", h)
	}
	body := LocalDay(at).Claim(Who("alice"), SlotList(9, 10))
	if *body.Zone != "Africa/Cairo" || *body.At != "2026-05-20" || body.Occurrence != nil {
		t.Fatalf("claim = %+v", body)
	}
}
