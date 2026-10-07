package schedulebit

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ontology-io/schedule-bit-go/api"
)

// Mask is a set of slots: bit n set means slot n. The API sends masks as hex
// strings ("0x600" is slots 9 and 10). A Mask is arbitrary-precision, so a
// minute World's 60 bits (or any wider leaf) never lose a slot. The zero
// value is the empty set; Masks are values, never mutated in place.
type Mask struct{ n big.Int }

// ParseMask reads the API's hex form ("0x600"); "0x0" and "" are empty.
func ParseMask(s string) (Mask, error) {
	var m Mask
	h := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(s)), "0x")
	if h == "" {
		return m, nil
	}
	if _, ok := m.n.SetString(h, 16); !ok || m.n.Sign() < 0 {
		return Mask{}, fmt.Errorf("schedulebit: %q is not a hex mask", s)
	}
	return m, nil
}

// MustMask is ParseMask that panics, for constants.
func MustMask(s string) Mask {
	m, err := ParseMask(s)
	if err != nil {
		panic(err)
	}
	return m
}

// MaskOf is the set of the given slots.
func MaskOf(slots ...int) Mask {
	var m Mask
	for _, s := range slots {
		if s >= 0 {
			m.n.SetBit(&m.n, s, 1)
		}
	}
	return m
}

// SlotRange is slots from..to inclusive.
func SlotRange(from, to int) Mask {
	var m Mask
	for s := from; s <= to; s++ {
		m.n.SetBit(&m.n, s, 1)
	}
	return m
}

// Hex is the API's form: "0x" and lower-case hex, "0x0" when empty.
func (m Mask) Hex() string { return "0x" + m.n.Text(16) }

// String is Hex.
func (m Mask) String() string { return m.Hex() }

// API is the mask as the generated types carry it.
func (m Mask) API() api.Mask { return m.Hex() }

// Slots lists the set slots, ascending.
func (m Mask) Slots() []int {
	var out []int
	for i := 0; i < m.n.BitLen(); i++ {
		if m.n.Bit(i) == 1 {
			out = append(out, i)
		}
	}
	return out
}

// Has reports whether slot s is set.
func (m Mask) Has(s int) bool { return s >= 0 && m.n.Bit(s) == 1 }

// Count is how many slots are set.
func (m Mask) Count() int {
	c := 0
	for _, w := range m.n.Bits() {
		for x := uint64(w); x != 0; x &= x - 1 {
			c++
		}
	}
	return c
}

// Empty reports whether no slot is set.
func (m Mask) Empty() bool { return m.n.Sign() == 0 }

// Equal reports whether both hold the same slots.
func (m Mask) Equal(o Mask) bool { return m.n.Cmp(&o.n) == 0 }

// Union is the slots in either.
func (m Mask) Union(o Mask) Mask { var r Mask; r.n.Or(&m.n, &o.n); return r }

// Intersect is the slots in both.
func (m Mask) Intersect(o Mask) Mask { var r Mask; r.n.And(&m.n, &o.n); return r }

// Without is the slots in m but not in o.
func (m Mask) Without(o Mask) Mask { var r Mask; r.n.AndNot(&m.n, &o.n); return r }

// SlotsInput is the mask as a request's slots field.
func (m Mask) SlotsInput() api.SlotsInput {
	var in api.SlotsInput
	_ = in.FromMask(m.Hex())
	return in
}

// SlotList is a request's slots field from a slot list.
func SlotList(slots ...int) api.SlotsInput {
	var in api.SlotsInput
	_ = in.FromSlots(append(api.Slots{}, slots...))
	return in
}

// Who names an occupier by its unique name.
func Who(name string) api.OccupierRef {
	var r api.OccupierRef
	_ = r.FromOccupierRef1(name)
	return r
}

// WhoID names an occupier by its id.
func WhoID(id int) api.OccupierRef {
	var r api.OccupierRef
	_ = r.FromOccupierRef0(id)
	return r
}
