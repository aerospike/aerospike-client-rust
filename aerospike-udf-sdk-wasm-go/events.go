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
// a function taking the current record, the incoming record and the conflict, and returning a Resolution
// and an error; the generated export decodes the event, calls it and delivers the answer.
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

// KeepLocal keeps the local record; the incoming write is dropped.
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
		ResultStr(r.verdict)
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
	rc := eventBinValue(r.Which, strptr(name), int32(len(name)), unsafe.Pointer(&v), 16)
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
	if eventBinValue(r.Which, strptr(name), int32(len(name)), unsafe.Pointer(&v), 16) != OK || v.Kind == KindNil {
		return Value{Kind: KindAbsent}, false
	}
	return v, true
}

// Has reports whether the side has the bin, nil included.
func (r *EventRecord) Has(name string) bool {
	var v Value
	return eventBinValue(r.Which, strptr(name), int32(len(name)), unsafe.Pointer(&v), 16) == OK
}

// Int is an int bin; false when absent, nil or of another kind.
func (r *EventRecord) Int(name string) (int64, bool) { return readInt(r, name) }

// Float64 is a double bin; false when absent, nil or of another kind. An integer bin is not widened.
func (r *EventRecord) Float64(name string) (float64, bool) { return readFloat64(r, name) }

// Bool is a boolean bin; false when absent, nil or of another kind.
func (r *EventRecord) Bool(name string) (bool, bool) { return readBool(r, name) }

// Str is the text of a string bin, copied out; false when absent, nil, of another kind, or unreadable.
func (r *EventRecord) Str(name string) (string, bool) { return readStr(r, name) }

// Bytes is the bytes of a blob bin, copied out; false when absent, nil or of another kind.
func (r *EventRecord) Bytes(name string) ([]byte, bool) { return readBytes(r, name) }

// Generation is this side's modification count: the Gen field, as RecordReader asks for it.
func (r *EventRecord) Generation() int64 { return r.Gen }

// LastUpdateTime is when this side was last written: the LUT field, as RecordReader asks for it.
func (r *EventRecord) LastUpdateTime() int64 { return r.LUT }

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

// Names walks the record's bin names.
func (r *EventRecord) Names() iter.Seq[string] {
	return func(yield func(string) bool) {
		for v := range r.Bins.List() {
			if s, err := v.Str(); err == nil && !yield(s) {
				return
			}
		}
	}
}

// WithBins is overrides with every bin of this record it lacks put on top, by position: the Rust SDK's
// with_bins, for a record answer that changes a few bins and keeps the rest. It prefers one side, so an
// xdr-merge handler that uses it on the current record is not symmetric; Pair.WithNewer is the symmetric
// form. Returns overrides.
func (r *EventRecord) WithBins(overrides *Map) *Map {
	for name := range r.Names() {
		if overrides.Contains(name) {
			continue
		}
		if v, err := r.Bin(name); err == nil {
			overrides.Put(name, v)
		}
	}
	return overrides
}

// DeletedAny reports whether this side deleted any bin.
func (r *EventRecord) DeletedAny() bool {
	return r.Deleted.Kind == KindList && r.Deleted.Len > 0
}

//==========================================================
// The conflict, typed.
//

// InvolvesDelete reports whether either side deleted a bin: the incoming write carries a bin delete, or
// the current record has a bin tombstone. Resurrecting such a bin from the other side is the classic
// merge mistake.
func (c *Conflict) InvolvesDelete() bool { return c.Local.DeletedAny() || c.Remote.DeletedAny() }

// RemoteDeleted is the names of the bins the incoming write deletes, as a stored list.
func (c *Conflict) RemoteDeleted() Value { return c.Remote.Deleted }

// Pair is the two records side by side, for bin-wise merging.
func (c *Conflict) Pair() Pair { return Pair{Local: &c.Local, Remote: &c.Remote} }

// Pair is the two sides of a conflict, read bin by bin into a Both: the Go face of the Rust SDK's Pair.
type Pair struct {
	Local  *EventRecord
	Remote *EventRecord
}

// Both is a bin on both sides of a conflict, either of which may be missing, with the merges a resolver
// reaches for. Max, Min and Sum are functions, since a method cannot add a constraint the type lacks.
type Both[T any] struct {
	Local     T
	Remote    T
	HasLocal  bool
	HasRemote bool
	localLUT  int64
	remoteLUT int64
	localSrc  int64
	remoteSrc int64
}

func both[T any](p Pair, name string, l T, hasL bool, r T, hasR bool) Both[T] {
	return Both[T]{
		Local: l, Remote: r, HasLocal: hasL, HasRemote: hasR,
		localLUT: p.Local.BinLUT(name), remoteLUT: p.Remote.BinLUT(name),
		localSrc: p.Local.BinSrc(name), remoteSrc: p.Remote.BinSrc(name),
	}
}

// Int is an int bin on each side; a side where it is absent, nil or of another kind is missing.
func (p Pair) Int(name string) Both[int64] {
	l, hl := p.Local.Int(name)
	r, hr := p.Remote.Int(name)
	return both(p, name, l, hl, r, hr)
}

// Float64 is a double bin on each side; a side where it is absent, nil or of another kind, an integer
// included, is missing.
func (p Pair) Float64(name string) Both[float64] {
	l, hl := p.Local.Float64(name)
	r, hr := p.Remote.Float64(name)
	return both(p, name, l, hl, r, hr)
}

// Bool is a boolean bin on each side; a side where it is absent, nil or of another kind is missing.
func (p Pair) Bool(name string) Both[bool] {
	l, hl := p.Local.Bool(name)
	r, hr := p.Remote.Bool(name)
	return both(p, name, l, hl, r, hr)
}

// Str is the text of a string bin on each side, copied out; a side where it is absent, nil or of another
// kind is missing.
func (p Pair) Str(name string) Both[string] {
	l, hl := p.Local.Str(name)
	r, hr := p.Remote.Str(name)
	return both(p, name, l, hl, r, hr)
}

// Bin is the bin as a value on each side; absent and nil sides are missing.
func (p Pair) Bin(name string) Both[Value] {
	l, hl := p.Local.Get(name)
	r, hr := p.Remote.Get(name)
	return both(p, name, l, hl, r, hr)
}

// List is a list bin on each side, as stored values to range over.
func (p Pair) List(name string) Both[Value] {
	l, hl := p.Local.Get(name)
	r, hr := p.Remote.Get(name)
	return both(p, name, l, hl && l.Kind == KindList, r, hr && r.Kind == KindList)
}

// RemoteNewer reports whether the remote side wrote name later, per bin: the order the bin-lut policy uses.
func (p Pair) RemoteNewer(name string) bool {
	ll, rl := p.Local.BinLUT(name), p.Remote.BinLUT(name)
	return rl > ll || (rl == ll && p.Remote.BinSrc(name) > p.Local.BinSrc(name))
}

// WithNewer is overrides with every bin either side has that it lacks, from the side that wrote it later -
// which is what the policy would have kept - so a handler merges the bins it knows about and leaves the
// rest as they would have been. Symmetric: both DCs order the two sides the same way. Returns overrides.
func (p Pair) WithNewer(overrides *Map) *Map {
	for _, side := range [2]*EventRecord{p.Local, p.Remote} {
		for name := range side.Names() {
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

// RemoteNewer reports whether the remote side wrote this bin later: by the bin's LUT where the server
// keeps one, the record's otherwise, and on a tie by source id - the order the server's own bin
// convergence uses, so "newer" means the same on both DCs even when the clocks agreed to the millisecond.
func (b Both[T]) RemoteNewer() bool {
	return b.remoteLUT > b.localLUT || (b.remoteLUT == b.localLUT && b.remoteSrc > b.localSrc)
}

// Newer is the side written later; whichever exists when only one does.
func (b Both[T]) Newer() (T, bool) {
	if b.RemoteNewer() {
		return b.RemoteOrLocal()
	}
	return b.LocalOrRemote()
}

// Older is the side written earlier; whichever exists when only one does.
func (b Both[T]) Older() (T, bool) {
	if b.RemoteNewer() {
		return b.LocalOrRemote()
	}
	return b.RemoteOrLocal()
}

// Ordered is both sides as (older, newer), each with whether it exists: the order both DCs agree on.
func (b Both[T]) Ordered() (older T, hasOlder bool, newer T, hasNewer bool) {
	if b.RemoteNewer() {
		return b.Local, b.HasLocal, b.Remote, b.HasRemote
	}
	return b.Remote, b.HasRemote, b.Local, b.HasLocal
}

// LocalOrRemote is the local side when it exists, the remote side otherwise; false when neither exists.
func (b Both[T]) LocalOrRemote() (T, bool) {
	if b.HasLocal {
		return b.Local, true
	}
	return b.Remote, b.HasRemote
}

// RemoteOrLocal is the remote side when it exists, the local side otherwise; false when neither exists.
func (b Both[T]) RemoteOrLocal() (T, bool) {
	if b.HasRemote {
		return b.Remote, true
	}
	return b.Local, b.HasLocal
}

// Merge combines the two when both exist; the one that exists otherwise.
func (b Both[T]) Merge(f func(l, r T) T) (T, bool) {
	if b.HasLocal && b.HasRemote {
		return f(b.Local, b.Remote), true
	}
	return b.LocalOrRemote()
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
	if b.HasLocal {
		s += b.Local
	}
	if b.HasRemote {
		s += b.Remote
	}
	return s
}
