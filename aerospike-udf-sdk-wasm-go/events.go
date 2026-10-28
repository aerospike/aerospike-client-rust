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
)

//==========================================================
// Events, the typed tier: what a `//udf:on` handler reads and returns. A handler is a function with the
// kind's parameters returning the kind's answer and an error (or just an error for the kinds that answer
// nothing); the generated export decodes the event, calls it and delivers the answer.
//

// The answers.

// Answer is what the kinds' answer types have in common; Deliver sets it as the call's result.
type Answer struct {
	verdict string
	code    int64
	message string
	bins    int32
	ttl     int64
	err     error
}

// Deliver sets the answer as the result and returns the status an export returns. Called by the
// generated export.
func (a Answer) Deliver() int32 {
	if a.err != nil {
		return FailErr(a.err)
	}
	return answer(a.verdict, a.code, a.message, a.bins, a.ttl)
}

func (a Answer) Verdict() string { return a.verdict }

func withBinsOf(verdict string, m *Map, ttl int64) Answer {
	return Answer{verdict: verdict, bins: m.handle, ttl: ttl, err: m.err}
}

// Decision is what a before-write handler returns.
type Decision struct{ Answer }

func Accept() Decision { return Decision{Answer{verdict: "accept", ttl: -1}} }
func Reject(code int64, message string) Decision {
	return Decision{Answer{verdict: "reject", code: code, message: message, ttl: -1}}
}

// AcceptRecord writes these bins instead; a bin absent from the map, or nil, is deleted.
func AcceptRecord(bins *Map) Decision { return Decision{withBinsOf("record", bins, -1)} }

// AcceptRecordTTL is AcceptRecord with a TTL in seconds (0 for the namespace default).
func AcceptRecordTTL(bins *Map, ttl int64) Decision { return Decision{withBinsOf("record", bins, ttl)} }

// Gate is what a before-delete or before-truncate handler returns.
type Gate struct{ Answer }

func Allow() Gate { return Gate{Answer{verdict: "allow", ttl: -1}} }
func Refuse(code int64, message string) Gate {
	return Gate{Answer{verdict: "refuse", code: code, message: message, ttl: -1}}
}

// Expiry is what a before-expire handler returns.
type Expiry struct{ Answer }

func Expire() Expiry { return Expiry{Answer{verdict: "expire", ttl: -1}} }

// Keep keeps the record, with this many more seconds to live.
func Keep(ttl int64) Expiry { return Expiry{Answer{verdict: "keep", ttl: ttl}} }

// Eviction is what a before-evict handler returns.
type Eviction struct{ Answer }

func Evict() Eviction   { return Eviction{Answer{verdict: "evict", ttl: -1}} }
func Protect() Eviction { return Eviction{Answer{verdict: "protect", ttl: -1}} }

// ReadResult is what an after-read handler returns.
type ReadResult struct{ Answer }

func Unchanged() ReadResult           { return ReadResult{Answer{verdict: "unchanged", ttl: -1}} }
func ReadRecord(bins *Map) ReadResult { return ReadResult{withBinsOf("record", bins, -1)} }

// ShipDecision is what a before-ship handler returns.
type ShipDecision struct{ Answer }

func Ship() ShipDecision                { return ShipDecision{Answer{verdict: "ship", ttl: -1}} }
func Skip() ShipDecision                { return ShipDecision{Answer{verdict: "skip", ttl: -1}} }
func ShipRecord(bins *Map) ShipDecision { return ShipDecision{withBinsOf("record", bins, -1)} }

// Resolution is what an xdr-merge handler returns: keep the current record, apply the incoming write
// whole, let the namespace's policy decide, or the final record.
type Resolution struct{ Answer }

func KeepLocal() Resolution  { return Resolution{Answer{verdict: "keep-local", ttl: -1}} }
func TakeRemote() Resolution { return Resolution{Answer{verdict: "take-remote", ttl: -1}} }
func Policy() Resolution     { return Resolution{Answer{verdict: "policy", ttl: -1}} }

// Resolve is the final record: these bins, whole - a bin not in the map, or nil, is deleted - keeping
// the TTL the incoming write asked for.
func Resolve(bins *Map) Resolution { return Resolution{withBinsOf("record", bins, -1)} }

// ResolveTTL is Resolve with a TTL in seconds: 0 for the namespace default.
func ResolveTTL(bins *Map, ttl int64) Resolution { return Resolution{withBinsOf("record", bins, ttl)} }

// Deliver sets a resolution as the result; the record form is the merge's own answer shape.
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

// Done is the answer of the kinds that answer nothing; the generated export delivers it after a handler
// returning only an error.
func Done() int32 { return answer("ok", 0, "", 0, -1) }

//==========================================================
// The event record, typed.
//

// Get is the bin as a value; false when the side has no such bin or it is nil.
func (r *EventRecord) Get(name string) (Value, bool) {
	var v Value
	if r.Bin(name, &v) != OK || v.Kind == KindNil {
		return Value{Kind: KindAbsent}, false
	}
	return v, true
}

// Int is an int bin; false when absent, nil or of another kind.
func (r *EventRecord) Int(name string) (int64, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindInt {
		return 0, false
	}
	return v.Payload, true
}

func (r *EventRecord) Float64(name string) (float64, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindDouble {
		return 0, false
	}
	return v.Double(), true
}

func (r *EventRecord) Bool(name string) (bool, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindBool {
		return false, false
	}
	return v.Payload != 0, true
}

func (r *EventRecord) Str(name string) (string, bool) {
	v, ok := r.Get(name)
	if !ok || v.Kind != KindStr {
		return "", false
	}
	s, err := v.Str()
	return s, err == nil
}

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

// Has reports whether the side has the bin, nil included.
func (r *EventRecord) Has(name string) bool {
	var v Value
	return r.Bin(name, &v) == OK
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

// DeletedAny reports whether this side deleted any bin.
func (r *EventRecord) DeletedAny() bool {
	return r.Deleted.Kind == KindList && r.Deleted.Len > 0
}

// WithBins is overrides with every bin of this record it lacks put on top, by position: the Rust SDK's
// with_bins, for a record answer that changes a few bins and keeps the rest. It prefers one side, so an
// xdr-merge handler that uses it on the current record is not symmetric; Pair.WithNewer is the symmetric
// form. Returns overrides.
func (r *EventRecord) WithBins(overrides *Map) *Map {
	for name := range r.Names() {
		if overrides.Has(name) {
			continue
		}
		var v Value
		if r.Bin(name, &v) == OK {
			overrides.Put(name, v)
		}
	}
	return overrides
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

func (p Pair) Int(name string) Both[int64] {
	l, hl := p.Local.Int(name)
	r, hr := p.Remote.Int(name)
	return both(p, name, l, hl, r, hr)
}

func (p Pair) Float64(name string) Both[float64] {
	l, hl := p.Local.Float64(name)
	r, hr := p.Remote.Float64(name)
	return both(p, name, l, hl, r, hr)
}

func (p Pair) Bool(name string) Both[bool] {
	l, hl := p.Local.Bool(name)
	r, hr := p.Remote.Bool(name)
	return both(p, name, l, hl, r, hr)
}

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
			if overrides.Has(name) {
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

func (b Both[T]) LocalOrRemote() (T, bool) {
	if b.HasLocal {
		return b.Local, true
	}
	return b.Remote, b.HasRemote
}

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
