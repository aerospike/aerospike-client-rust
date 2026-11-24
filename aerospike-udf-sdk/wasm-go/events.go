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

// Resolution is what an xdr-merge handler returns: keep the current record (KeepLocal), apply the
// incoming write whole (TakeRemote), do what the server would have done without the handler (Policy),
// remove the record (DeleteRecord, Expunge), or the final record, bin by bin (Resolve). Make one with
// those functions. The zero Resolution is not an answer: return it only beside a non-nil error, which
// fails the handler.
//
// No answer carries a TTL. The server gives the merged record the later of the two records'
// expirations, and a record that never expires wins.
//
//	//udf:on xdr-merge keep_richer
//	func keepRicher(current, incoming *udf.EventRecord) (udf.Resolution, error) {
//		mine, ok, err := current.Int("balance")
//		if err != nil {
//			return udf.Resolution{}, err // a balance that is not an integer fails the handler
//		}
//		if !ok {
//			return udf.Policy(), nil // nothing to compare: as if there were no handler
//		}
//		theirs, ok, err := incoming.Int("balance")
//		if err != nil {
//			return udf.Resolution{}, err
//		}
//		if ok && theirs > mine {
//			return udf.TakeRemote(), nil
//		}
//		return udf.KeepLocal(), nil
//	}
type Resolution struct {
	verdict string
	bins    int32
	err     error
}

// Verdict is the resolution's verdict word, as the server reads it: "keep-local", "take-remote",
// "policy", "delete", "expunge", or "record" for one made by Resolve.
func (r Resolution) Verdict() string { return r.verdict }

// KeepLocal keeps the current record as it is. The incoming write is dropped, which its source counts
// as a success.
func KeepLocal() Resolution { return Resolution{verdict: "keep-local"} }

// TakeRemote applies the incoming write whole, its bins over the current record, as if it had won
// every bin.
func TakeRemote() Resolution { return Resolution{verdict: "take-remote"} }

// Policy does what the server would have done without the handler: the answer of a handler that has
// no opinion on this pair. A set bound to a handler never uses bin convergence, so this applies the
// incoming write, as TakeRemote does.
func Policy() Resolution { return Resolution{verdict: "policy"} }

// DeleteRecord removes the record on this cluster and leaves a durable tombstone, so a late write
// cannot bring it back. The delete is not shipped: the other cluster decides for itself, so a merge
// that deletes has to delete on both sides for the two to agree. A record resolution with no bins
// left in it is not this: it is the handler's failure.
func DeleteRecord() Resolution { return Resolution{verdict: "delete"} }

// Expunge removes the record on this cluster outright, with no tombstone, so a later write creates
// it again. Like DeleteRecord, it is not shipped to the other cluster.
func Expunge() Resolution { return Resolution{verdict: "expunge"} }

// Resolve is the final record: these bins and no others. A bin that is not in the map, or is nil in
// it, is deleted, so a handler that decides a few bins fills in the rest first, with Pair.WithNewer.
//
// The server checks the answer before taking it, and what is wrong with it is the handler's failure:
// a key that is not a bin name (a string of 1 to 15 bytes), text in a GeoJSON bin that is not GeoJSON,
// and a map with no bins left, since a record of nothing is not a deletion (DeleteRecord is). A
// refusal the map kept while it was built fails the handler too.
//
//	pair := udf.PairOf(current, incoming)
//	out := udf.NewMap(4)
//	visits, err := pair.Int("visits")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if most, ok := udf.Max(visits); ok {
//		out.Put("visits", most)
//	}
//	return udf.Resolve(pair.WithNewer(out)), out.Err()
func Resolve(bins *Map) Resolution {
	return Resolution{verdict: "record", bins: bins.handle, err: bins.err}
}

// Deliver sets the resolution as the call's result and returns the status the export returns: 0, or
// non-zero after failing the call when the bins map had kept a refusal or the answer could not be
// built. The generated export calls it; a handler in the raw form ends with it.
//
//	return udf.KeepLocal().Deliver()
func (r Resolution) Deliver() int32 {
	if r.err != nil {
		return FailErr(r.err)
	}
	if r.bins == 0 {
		ResultString(r.verdict)
		return 0
	}
	return resolveRecord(r.bins)
}

//==========================================================
// The event record, typed.
//

// An EventRecord reads like a Record: both are RecordReaders, so a helper written over that interface
// takes either. It has no writing methods; an event changes a record through its answer.
var _ RecordReader = (*EventRecord)(nil)
var _ RecordReader = (*Record)(nil)

// Bin is one of this side's bins, whatever it holds. It is the form for a bin that must be there: the
// error names the bin, with Status NotFound, when the side has none by that name. A bin the side
// deleted, a nil bin, and a name that is not a bin name are all absent.
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

// Get is a bin that may be absent, whatever it holds: the value and true, or a Value of KindAbsent
// and false, with a nil error, when the side has no such bin or the bin is nil. The error is a bin
// that could not be read at all, as Record.Get's is.
func (r *EventRecord) Get(name string) (Value, bool, error) {
	var v Value
	rc := eventBinValue(r.which, strptr(name), int32(len(name)), unsafe.Pointer(&v), 16)
	return binValueOf(name, v, rc)
}

// Has reports whether the side has the bin. A bin the side deleted reads as absent; Deleted and
// DeletedBins are what tell "removed it" from "never had it".
func (r *EventRecord) Has(name string) bool {
	_, ok, _ := r.Get(name)
	return ok
}

// Int is an integer bin, with the three outcomes of every typed reader: the value and true; false
// with a nil error when the bin is absent or nil; an error with Status TypeMismatch when it holds
// another kind.
func (r *EventRecord) Int(name string) (int64, bool, error) { return readInt(r, name) }

// Double is a double bin; false when absent or nil, an error when of another kind. An integer bin is
// another kind: it is not widened.
func (r *EventRecord) Double(name string) (float64, bool, error) { return readDouble(r, name) }

// Bool is a boolean bin. The second result is false when the bin is absent or nil; a bin of another
// kind is an error.
func (r *EventRecord) Bool(name string) (bool, bool, error) { return readBool(r, name) }

// String is the text of a string bin, copied into a new Go string; false when absent or nil, an
// error when of another kind, a GeoJSON bin included.
func (r *EventRecord) String(name string) (string, bool, error) { return readString(r, name) }

// Blob is the bytes of a blob bin, copied into a new slice; false when absent or nil. It is strict:
// a string, GeoJSON or HLL bin is an error, like any other kind. Bytes reads the raw bytes of all
// four.
func (r *EventRecord) Blob(name string) ([]byte, bool, error) { return readBlob(r, name) }

// Bytes is the raw bytes of a string, blob, GeoJSON or HLL bin, copied into a new slice; false when
// absent or nil, an error when the bin is a scalar, a list or a map.
func (r *EventRecord) Bytes(name string) ([]byte, bool, error) { return readBytes(r, name) }

// IntOr is an integer bin, or dflt when it is absent or nil; a bin of another kind is an error, not
// the default.
func (r *EventRecord) IntOr(name string, dflt int64) (int64, error) { return readIntOr(r, name, dflt) }

// DoubleOr is a double bin, or dflt when it is absent or nil; a bin of another kind, an integer
// included, is an error.
func (r *EventRecord) DoubleOr(name string, dflt float64) (float64, error) {
	return readDoubleOr(r, name, dflt)
}

// BoolOr is a boolean bin, or dflt when it is absent or nil; a bin of another kind is an error.
func (r *EventRecord) BoolOr(name string, dflt bool) (bool, error) { return readBoolOr(r, name, dflt) }

// Generation is the record's generation as the event carries it, which for an xdr-merge is 0 on both
// sides: each cluster counts its own writes, and a merge that read the count could not come out the
// same on both. It is here because RecordReader has it.
func (r *EventRecord) Generation() int64 { return r.generation }

// LastUpdateTime is the record's last update time as the event carries it, in milliseconds since the
// Citrusleaf epoch, 2010-01-01T00:00:00Z (Unix milliseconds minus 1 262 304 000 000), which for an
// xdr-merge is 0 on both sides, for the same reason as Generation. To order the two sides, use
// BinLastUpdateTime, bin by bin.
func (r *EventRecord) LastUpdateTime() int64 { return r.lastUpdateTime }

// SourceID is the XDR source id of the cluster that wrote this side, the tie-break when two last
// update times are equal. It is never 0 on the incoming side: the server refuses an incoming write
// that carries no source id, or this cluster's own, before the handler runs.
func (r *EventRecord) SourceID() int64 { return r.sourceID }

// List is a list bin, as the stored Value to range over with Value.List; false when the bin is
// absent or nil, an error with Status TypeMismatch when it is not a list, and the list with Len 0
// when it is empty.
func (r *EventRecord) List(name string) (Value, bool, error) {
	return readKind(r, name, KindList, "a list")
}

// Map is a map bin, as the stored Value to range over with Value.Map or read with Value.Lookup;
// false when the bin is absent or nil, an error with Status TypeMismatch when it is not a map.
func (r *EventRecord) Map(name string) (Value, bool, error) {
	return readKind(r, name, KindMap, "a map")
}

// names walks a stored list of bin names as strings; nothing when the value is not a list. The names
// are the server's, so every element is a string; one that cannot be read fails the call, as a walk
// that cannot go on does, since a name passed over would be a bin lost.
func names(list Value) iter.Seq[string] {
	return func(yield func(string) bool) {
		for v := range list.List() {
			s, err := v.String()
			if err != nil {
				walkFailed("bin names", StatusOf(err))
				return
			}
			if !yield(s) {
				return
			}
		}
	}
}

// BinNames walks the names of this side's bins, each copied into a Go string. An incoming write that
// is partial (IsPartial) names only the bins it carries.
//
//	for name := range incoming.BinNames() {
//		if !out.Contains(name) {
//			v, err := incoming.Bin(name)
//			if err != nil {
//				return udf.Resolution{}, err
//			}
//			out.Put(name, v)
//		}
//	}
func (r *EventRecord) BinNames() iter.Seq[string] { return names(r.binNames) }

// BinCount is how many bins this side has, known without a walk; 0 for an incoming delete.
func (r *EventRecord) BinCount() int64 {
	if r.binNames.Kind != KindList {
		return 0
	}
	return int64(r.binNames.Len)
}

// WithBins fills overrides in from this record: every bin of the record that the map does not hold
// yet is put in, by position, and the same map is returned. It is for a record answer that changes a
// few bins and keeps the rest. It costs a few imports for each bin of the record.
//
// It prefers one side, so an xdr-merge handler that uses it on the current record is not symmetric:
// the other cluster, doing the same, keeps its own bins. Pair.WithNewer is the symmetric form; use
// this one when the side was chosen by something both clusters agree on.
//
// Only this record's own bins go in, so a bin this side deleted is not brought back from anywhere:
// it is not among BinNames, and the answer leaves it out. A bin that is named and cannot be read is
// a refusal the map keeps, answered by its Err, as a put that failed is.
func (r *EventRecord) WithBins(overrides *Map) *Map {
	for name := range r.BinNames() {
		if overrides.Contains(name) {
			continue
		}
		v, ok, err := r.Get(name)
		if err != nil {
			if overrides.err == nil {
				overrides.err = err
			}
			continue
		}
		if ok {
			overrides.Put(name, v)
		}
	}
	return overrides
}

// DeletedBins walks the names of the bins this side deleted: for the incoming write, its bin deletes;
// for the current record, its bin tombstones. Nothing when it deleted none. A deleted bin reads as
// absent, so this is the only way to tell "never had it" from "removed it", and BinLastUpdateTime
// says when. Bringing such a bin back from the other side is the mistake a merge has to guard
// against; Pair.WithNewer guards against it.
func (r *EventRecord) DeletedBins() iter.Seq[string] { return names(r.deletedBins) }

// Deleted reports whether this side deleted the bin name: whether it is among DeletedBins. The name
// is compared whole.
func (r *EventRecord) Deleted(name string) bool { return Contains(r.deletedBins, name) }

// DeletedAny reports whether this side deleted any bin, without walking DeletedBins.
func (r *EventRecord) DeletedAny() bool {
	return r.deletedBins.Kind == KindList && r.deletedBins.Len > 0
}

// IsDelete reports whether the incoming write is a record delete, in which case the incoming record has
// no bins. Always false of the current record.
func (r *EventRecord) IsDelete() bool { return r.isDelete }

// IsPartial reports whether the incoming write carries only the bins that changed, so a bin it lacks may
// well exist on the other cluster. Always false of the current record.
func (r *EventRecord) IsPartial() bool { return r.partial }

// IsTombstone reports whether the current record is a tombstone: it exists and has no bins. Always false
// of the incoming record, whose delete is IsDelete.
func (r *EventRecord) IsTombstone() bool { return r.tombstone }

//==========================================================
// The two records, side by side.
//

// Pair is the two records of a merge side by side, read bin by bin into a Both: the Go face of the
// Rust SDK's Pair. Make one with PairOf. Its readers take a bin name and answer that bin on each
// side, or an error when a side holds another kind than the reader's; Both then carries the merges.
//
//	//udf:on xdr-merge merge_account
//	func mergeAccount(current, incoming *udf.EventRecord) (udf.Resolution, error) {
//		pair := udf.PairOf(current, incoming)
//		out := udf.NewMap(8)
//
//		visits, err := pair.Int("visits")
//		if err != nil {
//			return udf.Resolution{}, err // a side where visits is not an integer
//		}
//		if most, ok := udf.Max(visits); ok { // both sides' increments are real
//			out.Put("visits", most)
//		}
//
//		profile, err := pair.Bin("profile")
//		if err != nil {
//			return udf.Resolution{}, err
//		}
//		if latest, ok := profile.Newer(); ok { // last writer wins
//			out.Put("profile", latest)
//		}
//
//		return udf.Resolve(pair.WithNewer(out)), out.Err() // the rest to the last writer of each
//	}
type Pair struct {
	Current  *EventRecord
	Incoming *EventRecord
}

// PairOf is the two records a handler was given, side by side, for bin-wise merging. Pass them as
// the handler got them, current first: Both's Current and Incoming follow the order given here.
func PairOf(current, incoming *EventRecord) Pair { return Pair{Current: current, Incoming: incoming} }

// Both is one bin on both sides of a merge, either of which may be missing, with the merges a resolver
// reaches for. Current and Incoming are the two values, and HasCurrent and HasIncoming say whether
// each is there; the value of a missing side is T's zero value and is not to be read. Both also
// remembers when, and by which cluster, each side wrote the bin, which is what Newer, Older and
// Ordered go by.
//
// Max, Min and Sum are functions, since a method cannot add a constraint the type lacks.
//
//	visits, err := pair.Int("visits") // a Both[int64]
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if visits.HasCurrent && visits.HasIncoming {
//		out.Put("visits", max(visits.Current, visits.Incoming))
//	}
//
//	note, err := pair.String("note")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if latest, ok := note.Newer(); ok { // last writer wins, in one step
//		out.Put("note", latest)
//	}
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

// pairOf reads one bin on each side with a reader of EventRecord.
func pairOf[T any](p Pair, name string, read func(*EventRecord, string) (T, bool, error)) (Both[T], error) {
	l, hl, err := read(p.Current, name)
	if err != nil {
		return Both[T]{}, err
	}
	r, hr, err := read(p.Incoming, name)
	if err != nil {
		return Both[T]{}, err
	}
	return both(p, name, l, hl, r, hr), nil
}

// Int is an integer bin on each side; a side where it is absent or nil is missing. A side where the
// bin holds another kind is an error, an *Error with Status TypeMismatch, as it is for
// EventRecord.Int: every reader of Pair answers the three outcomes of the reader of the same name,
// once for each side.
//
//	visits, err := pair.Int("visits")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if most, ok := udf.Max(visits); ok {
//		out.Put("visits", most)
//	}
func (p Pair) Int(name string) (Both[int64], error) {
	return pairOf(p, name, (*EventRecord).Int)
}

// Double is a double bin on each side; a side where it is absent or nil is missing, and a side where
// it holds another kind, an integer included, is an error.
func (p Pair) Double(name string) (Both[float64], error) {
	return pairOf(p, name, (*EventRecord).Double)
}

// Bool is a boolean bin on each side; a side where it is absent or nil is missing, and a side where
// it holds another kind is an error.
func (p Pair) Bool(name string) (Both[bool], error) {
	return pairOf(p, name, (*EventRecord).Bool)
}

// String is the text of a string bin on each side, copied out; a side where it is absent or nil is
// missing, and a side where it holds another kind is an error.
func (p Pair) String(name string) (Both[string], error) {
	return pairOf(p, name, (*EventRecord).String)
}

// Blob is the bytes of a blob bin on each side, copied out; a side where it is absent or nil is
// missing, and a side where it holds another kind - a string, GeoJSON or HLL included - is an error.
func (p Pair) Blob(name string) (Both[[]byte], error) {
	return pairOf(p, name, (*EventRecord).Blob)
}

// Bytes is the raw bytes of a string, blob, GeoJSON or HLL bin on each side, copied out; a side where
// it is absent or nil is missing, and a side where it is a scalar, a list or a map is an error.
func (p Pair) Bytes(name string) (Both[[]byte], error) {
	return pairOf(p, name, (*EventRecord).Bytes)
}

// Bin is the bin as a Value on each side, whatever kind it holds; a side where it is absent or nil
// is missing. It is the form for a bin the handler passes on without reading, as Newer does: the
// value goes into the answer by position. The error is a bin that could not be read at all.
func (p Pair) Bin(name string) (Both[Value], error) {
	return pairOf(p, name, (*EventRecord).Get)
}

// List is a list bin on each side, as stored Values to range over with Value.List; a side where the
// bin is absent or nil is missing, and a side where it is not a list is an error.
func (p Pair) List(name string) (Both[Value], error) {
	return pairOf(p, name, (*EventRecord).List)
}

// Map is a map bin on each side, as stored Values to range over with Value.Map or read with
// Value.Lookup; a side where the bin is absent or nil is missing, and a side where it is not a map
// is an error.
func (p Pair) Map(name string) (Both[Value], error) {
	return pairOf(p, name, (*EventRecord).Map)
}

// IncomingIsNewer reports whether the incoming side wrote the bin name later: by the bin's last
// update time, and on a tie by source id, the larger winning. It is symmetric: the other cluster,
// with the sides swapped, names the same winner.
func (p Pair) IncomingIsNewer(name string) bool {
	ll, rl := p.Current.BinLastUpdateTime(name), p.Incoming.BinLastUpdateTime(name)
	return rl > ll || (rl == ll && p.Incoming.BinSourceID(name) > p.Current.BinSourceID(name))
}

// WithNewer fills overrides in from both records: every bin either side has that the map does not
// hold yet is put in from the side that wrote it later, and the same map is returned. So a handler
// merges the bins it knows about and leaves the rest to the last writer of each. It is symmetric:
// both clusters order the two sides the same way.
//
// A bin only one side has comes from that side, whichever is newer, unless the newer side deleted
// it. A deleted bin is not brought back: when the side that wrote the bin later is the one that
// deleted it (Deleted), the map gets nil under the bin, which in a record answer deletes it. When
// the delete is the older write, the other side's value stands.
//
// A bin that is named and cannot be read is a refusal the map keeps, answered by its Err, as a put
// that failed is.
func (p Pair) WithNewer(overrides *Map) *Map {
	for _, side := range [2]*EventRecord{p.Current, p.Incoming} {
		for name := range side.BinNames() {
			if overrides.Contains(name) {
				continue
			}
			newer, older := p.Current, p.Incoming
			if p.IncomingIsNewer(name) {
				newer, older = p.Incoming, p.Current
			}
			if newer.Deleted(name) {
				overrides.Put(name, nil)
				continue
			}
			v, ok, err := newer.Get(name)
			if err == nil && !ok {
				v, ok, err = older.Get(name)
			}
			if err != nil {
				if overrides.err == nil {
					overrides.err = err
				}
				continue
			}
			if ok {
				overrides.Put(name, v)
			}
		}
	}
	return overrides
}

// IncomingIsNewer reports whether the incoming side wrote this bin later: by the bin's last update time,
// and on a tie by source id, so "newer" means the same on both DCs even when the clocks agreed to the
// millisecond. It goes by when each side wrote, not by whether a side has the bin.
func (b Both[T]) IncomingIsNewer() bool {
	return b.incomingTime > b.currentTime || (b.incomingTime == b.currentTime && b.incomingSource > b.currentSource)
}

// Newer is the value of the side that wrote the bin later, and true; the value of whichever side has
// the bin when only one does; false when neither has it. It is last-writer-wins for one bin. It goes
// by what each side holds, so a bin the newer side deleted comes from the older one: a handler that
// honours deletes asks EventRecord.Deleted first, as Pair.WithNewer does.
func (b Both[T]) Newer() (T, bool) {
	if b.IncomingIsNewer() {
		return b.IncomingOrCurrent()
	}
	return b.CurrentOrIncoming()
}

// Older is the value of the side that wrote the bin earlier, and true; the value of whichever side
// has the bin when only one does; false when neither has it.
func (b Both[T]) Older() (T, bool) {
	if b.IncomingIsNewer() {
		return b.CurrentOrIncoming()
	}
	return b.IncomingOrCurrent()
}

// Ordered is both sides as (older, newer), each with whether it is there: the order both DCs agree
// on, for a merge whose result depends on order, such as a list that is the older side's elements
// followed by the newer side's new ones.
//
//	both, err := pair.List("tags")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	older, hasOlder, newer, hasNewer := both.Ordered()
//	tags := udf.NewList(16)
//	if hasOlder {
//		tags.AddAll(older)
//	}
//	if hasNewer {
//		for tag := range newer.List() {
//			if text, err := tag.String(); err == nil && !(hasOlder && udf.Contains(older, text)) {
//				tags.Add(tag)
//			}
//		}
//	}
func (b Both[T]) Ordered() (older T, hasOlder bool, newer T, hasNewer bool) {
	if b.IncomingIsNewer() {
		return b.Current, b.HasCurrent, b.Incoming, b.HasIncoming
	}
	return b.Incoming, b.HasIncoming, b.Current, b.HasCurrent
}

// CurrentOrIncoming is the current side when it exists, the incoming side otherwise; false when neither
// exists. It prefers a side, so on its own it is not a symmetric merge.
func (b Both[T]) CurrentOrIncoming() (T, bool) {
	if b.HasCurrent {
		return b.Current, true
	}
	return b.Incoming, b.HasIncoming
}

// IncomingOrCurrent is the incoming side when it exists, the current side otherwise; false when neither
// exists. It prefers a side, so on its own it is not a symmetric merge.
func (b Both[T]) IncomingOrCurrent() (T, bool) {
	if b.HasIncoming {
		return b.Incoming, true
	}
	return b.Current, b.HasCurrent
}

// Merge combines the two values with f when both sides have the bin, and is the one that is there
// when only one does; false when neither has it. f is called with the current value first, and only
// when both exist. For the merge to converge f must not care which is which: f(a, b) has to equal
// f(b, a).
//
//	// The union of two flag words, the same from either side.
//	words, err := pair.Int("flags")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	flags, ok := words.Merge(func(a, b int64) int64 { return a | b })
func (b Both[T]) Merge(f func(l, r T) T) (T, bool) {
	if b.HasCurrent && b.HasIncoming {
		return f(b.Current, b.Incoming), true
	}
	return b.CurrentOrIncoming()
}

// Max is the larger of the two sides' values; the one that is there when only one side has the bin,
// and false when neither does. It is the merge for a count each cluster advanced on its own.
//
//	visits, err := pair.Int("visits")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if most, ok := udf.Max(visits); ok {
//		out.Put("visits", most)
//	}
func Max[T cmp.Ordered](b Both[T]) (T, bool) {
	return b.Merge(func(l, r T) T { return max(l, r) })
}

// Min is the smaller of the two sides' values; the one that is there when only one side has the bin,
// and false when neither does.
func Min[T cmp.Ordered](b Both[T]) (T, bool) {
	return b.Merge(func(l, r T) T { return min(l, r) })
}

// Sum is the sum of the two sides' values; the one that is there when only one side has the bin, a
// missing side counting as zero, and false when neither does, as Max and Min. An integer sum that
// overflows wraps around, as Go's addition does; it is never undefined. A sum is not idempotent: a
// merge that stores it and then meets the same write again adds it again. It is for adding up parts
// that belong to different clusters, never for merging two copies of one count, which is what Max is
// for.
func Sum[T int64 | float64](b Both[T]) (T, bool) {
	return b.Merge(func(l, r T) T { return l + r })
}

//==========================================================
// Merge building blocks: the merges of collections a convergent handler is built from. Each takes a
// Both[Value], as Pair.Map and Pair.List read one; a write UDF that merges two values of its own builds
// the Both by hand, a side it does not have left with HasCurrent or HasIncoming false.
//

// MaxPerKey is the per-key maximum of a map bin on both sides: a new map with every key of either side,
// each under the larger of its two values, or under its one value when only one side has the key. It is
// built on the host, so it is ordered by key and both clusters build the same bytes. It is false when
// neither side has the bin.
//
// A side that is missing, absent or nil does not count. A side that is there must be a map whose keys
// are strings or integers and whose values are integers; anything else is an error with Status
// TypeMismatch, a side that is not a map included: a merge does not guess what a double or a nested map
// should become. Status Refused is the host not building the map.
//
//	counts, err := pair.Map("counts")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	merged, ok, err := udf.MaxPerKey(counts)
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if ok {
//		out.Put("counts", merged)
//	}
//
// It is symmetric (the same map with the sides swapped), idempotent (merging a map with itself, or with
// a merge it is part of, changes nothing) and associative (three sides merge to the same map in any
// order): the laws a convergent merge needs. It is the merge of any map of values that only grow;
// CounterMerge is the same merge under the name of the structure it serves.
func MaxPerKey(b Both[Value]) (*Map, bool, error) {
	a, hasA, err := mergeSide(b.Current, b.HasCurrent, KindMap, "a map")
	if err != nil {
		return nil, false, err
	}
	c, hasC, err := mergeSide(b.Incoming, b.HasIncoming, KindMap, "a map")
	if err != nil {
		return nil, false, err
	}
	if !hasA && !hasC {
		return nil, false, nil
	}

	out := NewMap(int(a.Len + c.Len))
	pairs := make([]Value, 0, 2*int(a.Len+c.Len))

	// Every key of the first side, under the larger of its values on the two sides.
	if hasA {
		for key, value := range a.Map() {
			most, err := value.Int()
			if err == nil {
				err = mergeKey(key)
			}
			if err == nil && hasC {
				var other Value
				var ok bool
				other, ok, err = valueUnder(c, key)
				if err == nil && ok {
					var n int64
					n, err = other.Int()
					most = max(most, n)
				}
			}
			if err != nil {
				out.Close()
				return nil, false, err
			}
			pairs = append(pairs, key, IntValue(most))
		}
	}

	// Then the keys only the second side has.
	if hasC {
		for key, value := range c.Map() {
			count, err := value.Int()
			if err == nil {
				err = mergeKey(key)
			}
			var ok bool
			if err == nil && hasA {
				_, ok, err = valueUnder(a, key)
			}
			if err != nil {
				out.Close()
				return nil, false, err
			}
			if !ok {
				pairs = append(pairs, key, IntValue(count))
			}
		}
	}

	if err := out.PutEntries(pairs).Err(); err != nil {
		out.Close()
		return nil, false, err
	}
	return out, true, nil
}

// SortedUnion is the union of a list bin on both sides as a set: a new list with every element of
// either side once, sorted, so both clusters build the same bytes whatever order the elements arrived
// in. It is false when neither side has the bin.
//
// The elements are integers, strings or blobs: the kinds the server keys a map by, since the host sorts
// them as a map built on it sorts its keys. Integers come first, by value, then strings and then blobs,
// each by its bytes. An integer and a string are never the same element, however they read.
//
// A side that is missing, absent or nil does not count. A side that is there must be a list of those
// kinds; anything else is an error with Status TypeMismatch: a side that is not a list, or an element
// of another kind (a double, a nested list or map, nil). Status Refused is the host not building the
// list.
//
//	tags, err := pair.List("tags")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	union, ok, err := udf.SortedUnion(tags)
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if ok {
//		out.Put("tags", union)
//	}
//
// It is symmetric, idempotent and associative: the merge of a set that only grows. A removal needs a
// second set, as the 2P-Set of the CRDT tutorial keeps one.
func SortedUnion(b Both[Value]) (*List, bool, error) {
	a, hasA, err := mergeSide(b.Current, b.HasCurrent, KindList, "a list")
	if err != nil {
		return nil, false, err
	}
	c, hasC, err := mergeSide(b.Incoming, b.HasIncoming, KindList, "a list")
	if err != nil {
		return nil, false, err
	}
	if !hasA && !hasC {
		return nil, false, nil
	}

	// A map built on the host keeps its keys once each and in order: the elements go in as its keys,
	// and come out as the list.
	pairs := make([]Value, 0, 2*int(a.Len+c.Len))
	for _, s := range [2]Value{a, c} {
		for e := range s.List() {
			if e.Kind != KindInt && e.Kind != KindStr && e.Kind != KindBlob {
				return nil, false, e.kindError("an integer, a string or a blob")
			}
			pairs = append(pairs, e, BoolValue(true))
		}
	}

	set := NewMap(len(pairs) / 2).PutEntries(pairs)
	defer set.Close()
	if err := set.Err(); err != nil {
		return nil, false, err
	}

	out := NewList(set.Len())
	if n := set.Len(); n > 0 {
		src := packHandle(set.handle)
		if src <= 0 {
			out.Close()
			return nil, false, errorf(Refused, "could not read the union back: the call has spent the memory it may hold on the server")
		}
		keys := make([]Value, 0, n)
		for key := range (Value{Kind: KindMap, Len: uint32(n), Payload: int64(src) << 32}).Map() {
			keys = append(keys, key)
		}
		out.AddValues(keys)
	}
	if err := out.Err(); err != nil {
		out.Close()
		return nil, false, err
	}
	return out, true, nil
}

// CounterMerge is the merge of a per-origin counter: a map of origin to count, each origin's count
// raised only by the cluster it names, merged by the larger count per origin. It is MaxPerKey under the
// name of the structure: the same rules, the same errors, and false when neither side has the bin.
// CounterTotal reads the counter's value.
//
//	hits, err := pair.Map("hits")
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	merged, ok, err := udf.CounterMerge(hits)
//
// It is symmetric, idempotent and associative. A count that goes down is not a count this merges: a
// PN-Counter keeps a second counter for the decrements.
func CounterMerge(b Both[Value]) (*Map, bool, error) {
	return MaxPerKey(b)
}

// CounterTotal is the value of a per-origin counter: the sum of its counts, wrapping on overflow. It is 0
// for a counter that is absent or nil. The error has Status TypeMismatch when the value is not a map,
// or when a key is not a string or an integer or a count is not an integer, as for CounterMerge. A
// built counter (CounterMerge's answer) is read with its Value.
//
//	hits, _, err := rec.Map("hits")
//	if err != nil {
//		return 0, err
//	}
//	return udf.CounterTotal(hits)
func CounterTotal(v Value) (int64, error) {
	if v.Kind == KindHandle {
		src := packHandle(int32(v.Payload))
		if src <= 0 {
			return 0, errorf(Refused, "could not read the counter: the call has spent the memory it may hold on the server")
		}
		n := collSize(int32(v.Payload))
		if n <= 0 {
			return 0, nil
		}
		v = Value{Kind: KindMap, Len: uint32(n), Payload: int64(src) << 32}
	}
	m, has, err := mergeSide(v, true, KindMap, "a map")
	if err != nil || !has {
		return 0, err
	}
	var total int64
	for key, count := range m.Map() {
		if err := mergeKey(key); err != nil {
			return 0, err
		}
		n, err := count.Int()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// LwwByVersion is last writer wins by an application version, on a bin that holds a [version, value]
// list on both sides: the side with the higher version, as it is, to write back. Two equal versions are
// decided by the lists' stored bytes, the greater winning, so both clusters pick the same side; two
// lists with the same bytes are the same either way. It is false when neither side has the bin.
//
// A side that is missing, absent or nil does not count. A side that is there must be a list of exactly
// two elements whose first is an integer; anything else is an error with Status TypeMismatch. The value
// may be of any kind.
//
//	price, err := pair.List("price") // the write UDF sets [version + 1, price] in one write
//	if err != nil {
//		return udf.Resolution{}, err
//	}
//	if winner, ok, err := udf.LwwByVersion(price); err != nil {
//		return udf.Resolution{}, err
//	} else if ok {
//		out.Put("price", winner)
//	}
//
// It is symmetric, idempotent and associative: it is the larger of the two by version, then by bytes,
// which is a total order. The version is the application's, raised by every write that changes the
// value, so no clock decides.
func LwwByVersion(b Both[Value]) (Value, bool, error) {
	none := Value{Kind: KindAbsent}
	a, hasA, err := mergeSide(b.Current, b.HasCurrent, KindList, "a list")
	if err != nil {
		return none, false, err
	}
	c, hasC, err := mergeSide(b.Incoming, b.HasIncoming, KindList, "a list")
	if err != nil {
		return none, false, err
	}
	var va, vc int64
	if hasA {
		if va, err = lwwVersion(a); err != nil {
			return none, false, err
		}
	}
	if hasC {
		if vc, err = lwwVersion(c); err != nil {
			return none, false, err
		}
	}
	switch {
	case !hasA && !hasC:
		return none, false, nil
	case !hasC:
		return a, true, nil
	case !hasA:
		return c, true, nil
	case va != vc:
		if va > vc {
			return a, true, nil
		}
		return c, true, nil
	}
	order, err := CmpStored(a, c)
	if err != nil {
		return none, false, err
	}
	if order >= 0 {
		return a, true, nil
	}
	return c, true, nil
}

// mergeSide is one side of a merge of collections: the value and true; false when it is missing,
// absent or nil; an error with Status TypeMismatch when it is of another kind than want.
func mergeSide(v Value, has bool, want Kind, what string) (Value, bool, error) {
	if !has || v.IsNull() {
		return Value{Kind: KindAbsent}, false, nil
	}
	if v.Kind != want {
		return Value{Kind: KindAbsent}, false, v.kindError(what)
	}
	return v, true, nil
}

// mergeKey is nil for a key a map merge takes: a string or an integer.
func mergeKey(key Value) error {
	if key.Kind != KindInt && key.Kind != KindStr {
		return key.kindError("a string or an integer key")
	}
	return nil
}

// valueUnder is the entry under key, a key read from another map, in the map m.
func valueUnder(m Value, key Value) (Value, bool, error) {
	if key.Kind == KindInt {
		return m.LookupInt(key.Payload)
	}
	text, err := key.String()
	if err != nil {
		return Value{Kind: KindAbsent}, false, err
	}
	return m.Lookup(text)
}

// lwwVersion is the version of a [version, value] list; an error with Status TypeMismatch for any other
// shape.
func lwwVersion(l Value) (int64, error) {
	if l.Len != 2 {
		return 0, errorf(TypeMismatch, "the value is not a [version, value] list")
	}
	first, ok, err := l.Index(0)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errorf(TypeMismatch, "the value is not a [version, value] list")
	}
	return first.Int()
}
