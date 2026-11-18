// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package udf

import (
	"cmp"
	"iter"
	"unsafe"
)

//==========================================================
// The xdr-merge event, the typed tier: what a `//udf:on xdr-merge` handler reads and returns. A handler is
// a function taking the current record and the incoming record, and returning a Resolution and an error;
// the generated export decodes the event, calls it and delivers the answer.
//

// Resolution is what an xdr-merge handler returns: keep the current record, apply the incoming write
// whole, let the namespace's policy decide, or the final record.
type Resolution struct {
	verdict string
	bins    int32
	ttl     int64
	err     error
}

// Verdict is the resolution's verdict word, such as "keep-local" or "record".
func (r Resolution) Verdict() string { return r.verdict }

func recordOf(m *Map, ttl int64) Resolution {
	return Resolution{verdict: "record", bins: m.handle, ttl: ttl, err: m.err}
}

// KeepLocal keeps the current record; the incoming write is dropped.
func KeepLocal() Resolution { return Resolution{verdict: "keep-local", ttl: -1} }

// TakeRemote applies the incoming write whole, as if it had won every bin.
func TakeRemote() Resolution { return Resolution{verdict: "take-remote", ttl: -1} }

// Policy lets the namespace's policy decide, as it would have without the event.
func Policy() Resolution { return Resolution{verdict: "policy", ttl: -1} }

// DeleteRecord removes the record on this cluster. The other cluster decides for itself, so a merge that
// deletes has to delete on both sides for the two to agree. A record resolution with no bins left in it
// is not this: it is the handler's failure.
func DeleteRecord() Resolution { return Resolution{verdict: "delete", ttl: -1} }

// Resolve is the final record: these bins, whole - a bin not in the map, or nil, is deleted - keeping
// the TTL the incoming write asked for.
func Resolve(bins *Map) Resolution { return recordOf(bins, -1) }

// ResolveTTL is Resolve with a TTL in seconds: 0 for the namespace default.
func ResolveTTL(bins *Map, ttl int64) Resolution { return recordOf(bins, ttl) }

// Deliver sets the resolution as the result and returns the status an export returns: the verdict word,
// or the record form with its bins and TTL. Called by the generated export.
func (r Resolution) Deliver() int32 {
	if r.err != nil {
		return FailErr(r.err)
	}
	if r.bins == 0 {
		ResultString(r.verdict)
		return 0
	}
	return resolveRecord(r.bins, r.ttl)
}

//==========================================================
// The event record, typed.
//

// An EventRecord reads like a Record: both are RecordReaders, so a helper written over that interface
// takes either. It has no writing methods; an event changes a record through its answer.
var _ RecordReader = (*EventRecord)(nil)
var _ RecordReader = (*Record)(nil)

// Bin is one of this side's bins, whatever it holds, a nil included; an error naming the bin when the
// side has none by that name.
func (r *EventRecord) Bin(name string) (Value, error) {
	var v Value
	rc := eventBinValue(r.which, strptr(name), int32(len(name)), unsafe.Pointer(&v), 16)
	if rc == NotFound {
		return Value{Kind: KindAbsent}, errorf(rc, "bin ", name, " is absent")
	}
	if rc != OK {
		return Value{Kind: KindAbsent}, statusError(rc, "bin "+name+" could not be read")
	}
	return v, nil
}

// Get is the bin as a value; false when the side has no such bin or it is nil.
func (r *EventRecord) Get(name string) (Value, bool) {
	var v Value
	if eventBinValue(r.which, strptr(name), int32(len(name)), unsafe.Pointer(&v), 16) != OK || v.Kind == KindNil {
		return Value{Kind: KindAbsent}, false
	}
	return v, true
}

// Has reports whether the side has the bin, nil included.
func (r *EventRecord) Has(name string) bool {
	var v Value
	return eventBinValue(r.which, strptr(name), int32(len(name)), unsafe.Pointer(&v), 16) == OK
}

// Int is an int bin; false when absent, nil or of another kind.
func (r *EventRecord) Int(name string) (int64, bool) { return readInt(r, name) }

// Double is a double bin; false when absent, nil or of another kind. An integer bin is not widened.
func (r *EventRecord) Double(name string) (float64, bool) { return readDouble(r, name) }

// Bool is a boolean bin; false when absent, nil or of another kind.
func (r *EventRecord) Bool(name string) (bool, bool) { return readBool(r, name) }

// String is the text of a string bin, copied out; false when absent, nil, of another kind, or unreadable.
func (r *EventRecord) String(name string) (string, bool) { return readString(r, name) }

// Blob is the bytes of a blob bin, copied out; false when absent, nil or of another kind.
func (r *EventRecord) Blob(name string) ([]byte, bool) { return readBlob(r, name) }

// IntOr is an int bin, or dflt when it is absent, nil or of another kind.
func (r *EventRecord) IntOr(name string, dflt int64) int64 { return readIntOr(r, name, dflt) }

// DoubleOr is a double bin, or dflt when it is absent, nil or of another kind, an integer included.
func (r *EventRecord) DoubleOr(name string, dflt float64) float64 { return readDoubleOr(r, name, dflt) }

// BoolOr is a boolean bin, or dflt when it is absent, nil or of another kind.
func (r *EventRecord) BoolOr(name string, dflt bool) bool { return readBoolOr(r, name, dflt) }

// Generation is this side's modification count.
func (r *EventRecord) Generation() int64 { return r.generation }

// LastUpdateTime is when this side was last written, in ms since the Aerospike epoch (2010-01-01 UTC).
func (r *EventRecord) LastUpdateTime() int64 { return r.lastUpdateTime }

// TTL is this side's time to live, in seconds: what the current record has left, and what the incoming
// write asks for.
func (r *EventRecord) TTL() int64 { return r.ttl }

// SourceID is the id of the cluster that wrote this side, the tie-break when two last update times are
// equal.
func (r *EventRecord) SourceID() int64 { return r.sourceID }

// List walks a list bin; nothing when absent or not a list.
func (r *EventRecord) List(name string) iter.Seq[Value] {
	v, _ := r.Get(name)
	return v.List()
}

// Map walks a map bin's entries; nothing when absent or not a map.
func (r *EventRecord) Map(name string) iter.Seq2[Value, Value] {
	v, _ := r.Get(name)
	return v.Map()
}

// names walks a stored list of bin names as strings; nothing when the value is not a list.
func names(list Value) iter.Seq[string] {
	return func(yield func(string) bool) {
		for v := range list.List() {
			if s, err := v.String(); err == nil && !yield(s) {
				return
			}
		}
	}
}

// BinNames walks the record's bin names.
func (r *EventRecord) BinNames() iter.Seq[string] { return names(r.binNames) }

// BinCount is how many bins this side has.
func (r *EventRecord) BinCount() int64 {
	if r.binNames.Kind != KindList {
		return 0
	}
	return int64(r.binNames.Len)
}

// WithBins is overrides with every bin of this record it lacks put on top, by position: the Rust SDK's
// with_bins, for a record answer that changes a few bins and keeps the rest. It prefers one side, so an
// xdr-merge handler that uses it on the current record is not symmetric; Pair.WithNewer is the symmetric
// form. Returns overrides.
func (r *EventRecord) WithBins(overrides *Map) *Map {
	for name := range r.BinNames() {
		if overrides.Contains(name) {
			continue
		}
		if v, err := r.Bin(name); err == nil {
			overrides.Put(name, v)
		}
	}
	return overrides
}

// DeletedBins walks the names of the bins this side deleted; nothing when it deleted none.
func (r *EventRecord) DeletedBins() iter.Seq[string] { return names(r.deletedBins) }

// DeletedAny reports whether this side deleted any bin.
func (r *EventRecord) DeletedAny() bool {
	return r.deletedBins.Kind == KindList && r.deletedBins.Len > 0
}

// IsDelete reports whether the incoming write is a record delete, in which case the incoming record has
// no bins. Always false of the current record.
func (r *EventRecord) IsDelete() bool { return r.isDelete }

// IsPartial reports whether the incoming write carries only the bins that changed, so a bin it lacks may
// well exist on the other cluster. Always false of the current record.
func (r *EventRecord) IsPartial() bool { return r.partial }

// LostBins walks the names of the incoming bins that would lose under bin convergence. Nothing of the
// current record.
func (r *EventRecord) LostBins() iter.Seq[string] { return names(r.lost) }

// IsTombstone reports whether the current record is a tombstone: it exists and has no bins. Always false
// of the incoming record, whose delete is IsDelete.
func (r *EventRecord) IsTombstone() bool { return r.tombstone }

// UsesBinConvergence reports whether the namespace converges bin by bin, which is when the per-bin
// timestamps behind BinLastUpdateTime mean something; without it that is the record's for every bin. A
// fact about the merge, so both records answer the same.
func (r *EventRecord) UsesBinConvergence() bool { return r.binConvergence }

//==========================================================
// The two records, side by side.
//

// Pair is the two records of a merge, read bin by bin into a Both: the Go face of the Rust SDK's Pair.
// Make one with PairOf.
type Pair struct {
	Current  *EventRecord
	Incoming *EventRecord
}

// PairOf is the two records a handler was given, side by side, for bin-wise merging.
func PairOf(current, incoming *EventRecord) Pair { return Pair{Current: current, Incoming: incoming} }

// Both is a bin on both sides of a merge, either of which may be missing, with the merges a resolver
// reaches for. Max, Min and Sum are functions, since a method cannot add a constraint the type lacks.
type Both[T any] struct {
	Current        T
	Incoming       T
	HasCurrent     bool
	HasIncoming    bool
	currentTime    int64
	incomingTime   int64
	currentSource  int64
	incomingSource int64
}

func both[T any](p Pair, name string, c T, hasC bool, i T, hasI bool) Both[T] {
	return Both[T]{
		Current: c, Incoming: i, HasCurrent: hasC, HasIncoming: hasI,
		currentTime: p.Current.BinLastUpdateTime(name), incomingTime: p.Incoming.BinLastUpdateTime(name),
		currentSource: p.Current.BinSourceID(name), incomingSource: p.Incoming.BinSourceID(name),
	}
}

// Int is an int bin on each side; a side where it is absent, nil or of another kind is missing.
func (p Pair) Int(name string) Both[int64] {
	l, hl := p.Current.Int(name)
	r, hr := p.Incoming.Int(name)
	return both(p, name, l, hl, r, hr)
}

// Double is a double bin on each side; a side where it is absent, nil or of another kind, an integer
// included, is missing.
func (p Pair) Double(name string) Both[float64] {
	l, hl := p.Current.Double(name)
	r, hr := p.Incoming.Double(name)
	return both(p, name, l, hl, r, hr)
}

// Bool is a boolean bin on each side; a side where it is absent, nil or of another kind is missing.
func (p Pair) Bool(name string) Both[bool] {
	l, hl := p.Current.Bool(name)
	r, hr := p.Incoming.Bool(name)
	return both(p, name, l, hl, r, hr)
}

// String is the text of a string bin on each side, copied out; a side where it is absent, nil or of
// another kind is missing.
func (p Pair) String(name string) Both[string] {
	l, hl := p.Current.String(name)
	r, hr := p.Incoming.String(name)
	return both(p, name, l, hl, r, hr)
}

// Blob is the bytes of a blob bin on each side, copied out; a side where it is absent, nil or of another
// kind is missing.
func (p Pair) Blob(name string) Both[[]byte] {
	l, hl := p.Current.Blob(name)
	r, hr := p.Incoming.Blob(name)
	return both(p, name, l, hl, r, hr)
}

// Bin is the bin as a value on each side; absent and nil sides are missing.
func (p Pair) Bin(name string) Both[Value] {
	l, hl := p.Current.Get(name)
	r, hr := p.Incoming.Get(name)
	return both(p, name, l, hl, r, hr)
}

// List is a list bin on each side, as stored values to range over.
func (p Pair) List(name string) Both[Value] {
	l, hl := p.Current.Get(name)
	r, hr := p.Incoming.Get(name)
	return both(p, name, l, hl && l.Kind == KindList, r, hr && r.Kind == KindList)
}

// Map is a map bin on each side, as stored values to range over.
func (p Pair) Map(name string) Both[Value] {
	l, hl := p.Current.Get(name)
	r, hr := p.Incoming.Get(name)
	return both(p, name, l, hl && l.Kind == KindMap, r, hr && r.Kind == KindMap)
}

// IncomingIsNewer reports whether the incoming side wrote name later, per bin: the order the bin-lut
// policy uses.
func (p Pair) IncomingIsNewer(name string) bool {
	ll, rl := p.Current.BinLastUpdateTime(name), p.Incoming.BinLastUpdateTime(name)
	return rl > ll || (rl == ll && p.Incoming.BinSourceID(name) > p.Current.BinSourceID(name))
}

// WithNewer is overrides with every bin either side has that it lacks, from the side that wrote it later -
// which is what the policy would have kept - so a handler merges the bins it knows about and leaves the
// rest as they would have been. Symmetric: both DCs order the two sides the same way. Returns overrides.
func (p Pair) WithNewer(overrides *Map) *Map {
	for _, side := range [2]*EventRecord{p.Current, p.Incoming} {
		for name := range side.BinNames() {
			if overrides.Contains(name) {
				continue
			}
			if v, ok := p.Bin(name).Newer(); ok {
				overrides.Put(name, v)
			}
		}
	}
	return overrides
}

// IncomingIsNewer reports whether the incoming side wrote this bin later: by the bin's last update time
// where the server keeps one, the record's otherwise, and on a tie by source id - the order the server's
// own bin convergence uses, so "newer" means the same on both DCs even when the clocks agreed to the
// millisecond.
func (b Both[T]) IncomingIsNewer() bool {
	return b.incomingTime > b.currentTime || (b.incomingTime == b.currentTime && b.incomingSource > b.currentSource)
}

// Newer is the side written later; whichever exists when only one does.
func (b Both[T]) Newer() (T, bool) {
	if b.IncomingIsNewer() {
		return b.IncomingOrCurrent()
	}
	return b.CurrentOrIncoming()
}

// Older is the side written earlier; whichever exists when only one does.
func (b Both[T]) Older() (T, bool) {
	if b.IncomingIsNewer() {
		return b.CurrentOrIncoming()
	}
	return b.IncomingOrCurrent()
}

// Ordered is both sides as (older, newer), each with whether it exists: the order both DCs agree on.
func (b Both[T]) Ordered() (older T, hasOlder bool, newer T, hasNewer bool) {
	if b.IncomingIsNewer() {
		return b.Current, b.HasCurrent, b.Incoming, b.HasIncoming
	}
	return b.Incoming, b.HasIncoming, b.Current, b.HasCurrent
}

// CurrentOrIncoming is the current side when it exists, the incoming side otherwise; false when neither exists.
func (b Both[T]) CurrentOrIncoming() (T, bool) {
	if b.HasCurrent {
		return b.Current, true
	}
	return b.Incoming, b.HasIncoming
}

// IncomingOrCurrent is the incoming side when it exists, the current side otherwise; false when neither exists.
func (b Both[T]) IncomingOrCurrent() (T, bool) {
	if b.HasIncoming {
		return b.Incoming, true
	}
	return b.Current, b.HasCurrent
}

// Merge combines the two when both exist; the one that exists otherwise.
func (b Both[T]) Merge(f func(l, r T) T) (T, bool) {
	if b.HasCurrent && b.HasIncoming {
		return f(b.Current, b.Incoming), true
	}
	return b.CurrentOrIncoming()
}

// Max is the larger of the two; the one that exists when only one does.
func Max[T cmp.Ordered](b Both[T]) (T, bool) {
	return b.Merge(func(l, r T) T { return max(l, r) })
}

// Min is the smaller of the two; the one that exists when only one does.
func Min[T cmp.Ordered](b Both[T]) (T, bool) {
	return b.Merge(func(l, r T) T { return min(l, r) })
}

// Sum is the sum, a missing side counting as zero.
func Sum[T int64 | float64](b Both[T]) T {
	var s T
	if b.HasCurrent {
		s += b.Current
	}
	if b.HasIncoming {
		s += b.Incoming
	}
	return s
}
