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

// The conformance module: the same exports, to the same specification, in every guest SDK, so that a
// harness can call all five with one record and one set of arguments and compare the answers. Each
// export uses this SDK's public API the way a user would and answers what the SDK answered; nothing here
// works around it.
//
// Most answers are a list whose first element is a tag: "some", "none", "wrong", or a status named
// after the host's codes ("ok", "err", "absent", "refused", "small"). The tag of a failure is the
// status its error carries, read with udf.StatusOf.
//
// Nothing here is an example to copy.
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go/cmd/udfgen

import (
	"errors"
	"math"
	"strconv"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go"
)

const point = `{"type":"Point","coordinates":[1,2]}`

// The most a builder is asked for, and the most integers or doubles one packed write takes.
const (
	mkMax     = 1_000_000
	packedMax = 256
)

// A user key is copied into a buffer of the caller's; this one holds the longest string the harness
// hands the module.
var keyBuf [65536]byte

// tag is an SDK error as the status it carries, named after the host's codes.
func tag(err error) string {
	switch status := udf.StatusOf(err); {
	case status == udf.OK:
		return "ok"
	case status == udf.NotFound:
		return "absent"
	case status == udf.TypeMismatch:
		return "wrong"
	case status == udf.Refused:
		return "refused"
	case status <= udf.NeededBase:
		return "small"
	}
	return "err"
}

// kind is what a value holds, by its kind tests.
func kind(v udf.Value) string {
	switch {
	case v.IsNil():
		return "nil"
	case v.IsBool():
		return "bool"
	case v.IsInt():
		return "int"
	case v.IsDouble():
		return "double"
	case v.IsString():
		return "string"
	case v.IsBlob():
		return "blob"
	case v.IsList():
		return "list"
	case v.IsMap():
		return "map"
	case v.IsGeoJSON():
		return "geojson"
	case v.IsHLL():
		return "hll"
	}
	return "other"
}

func some(v any) (*udf.List, error) {
	out := udf.NewList(2).Add("some").Add(v)
	return out, out.Err()
}

func only(tag string) (*udf.List, error) {
	out := udf.NewList(1).Add(tag)
	return out, out.Err()
}

// answer is the three outcomes of a reader as a tagged list: the value, "none" for nothing there, and
// the status of the error for a bin of another kind.
func answer(v any, ok bool, err error) (*udf.List, error) {
	if err != nil {
		return only(tag(err))
	}
	if !ok {
		return only("none")
	}
	return some(v)
}

// orAnswer is a reader with a default as a tagged list: the value, or the status of the error.
func orAnswer(v any, err error) (*udf.List, error) {
	if err != nil {
		return only(tag(err))
	}
	return some(v)
}

// Reads bin name with one reader of the record. A list or a map is answered as the stored value,
// passed back.
//
//udf:function rd
func rd(rec *udf.Record, name string, how string) (*udf.List, error) {
	switch how {
	case "has":
		return some(rec.Has(name))
	case "get":
		return answer(rec.Get(name))
	case "bin":
		return orAnswer(rec.Bin(name))
	case "int":
		return answer(rec.Int(name))
	case "double":
		return answer(rec.Double(name))
	case "bool":
		return answer(rec.Bool(name))
	case "string":
		return answer(rec.String(name))
	case "blob":
		return answer(rec.Blob(name))
	case "list":
		return answer(rec.List(name))
	case "map":
		return answer(rec.Map(name))
	case "int_or":
		return orAnswer(rec.IntOr(name, 7))
	case "double_or":
		return orAnswer(rec.DoubleOr(name, 7.5))
	case "bool_or":
		return orAnswer(rec.BoolOr(name, true))
	}
	return nil, errors.New("rd: unknown how")
}

// ints is the integers of a list of at most packedMax integers, and whether v is one.
func ints(v udf.Value) ([]int64, bool) {
	if !v.IsList() || v.Len > packedMax {
		return nil, false
	}
	out := make([]int64, 0, v.Len)
	for e := range v.List() {
		n, err := e.Int()
		if err != nil {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}

// doubles is the doubles of a list of at most packedMax doubles, and whether v is one.
func doubles(v udf.Value) ([]float64, bool) {
	if !v.IsList() || v.Len > packedMax {
		return nil, false
	}
	out := make([]float64, 0, v.Len)
	for e := range v.List() {
		d, err := e.Double()
		if err != nil {
			return nil, false
		}
		out = append(out, d)
	}
	return out, true
}

// Stages one write and saves: the status of the staging call, the status of Save ("skipped" when
// staging failed), and BinCount afterwards. An argument that is missing, or of a kind the write cannot
// take, is the staging status "wrong", and nothing is written.
//
// An optional parameter is nil for an argument that was not passed and for one passed as nil alike, so
// "set" with no v is "set" with nil, which removes the bin.
//
//udf:function wr
func wr(rec *udf.Record, name string, how string, v []udf.Value) (*udf.List, error) {
	// The value is taken as the rest of the arguments, so that one left out (none) is told from one
	// passed as nil (a nil): an optional parameter is nil for both.
	if len(v) > 1 {
		return nil, errors.New("wr takes at most 3 arguments")
	}

	missing := len(v) == 0
	arg := udf.NilValue()
	if !missing {
		arg = v[0]
	}

	stage := "wrong"

	switch how {
	case "set":
		if !missing {
			stage = tag(rec.Set(name, arg))
		}
	case "blob":
		if arg.IsString() || arg.IsBlob() {
			b, err := arg.Bytes()
			if err == nil {
				err = rec.SetBlob(name, b)
			}
			stage = tag(err)
		}
	case "geojson":
		s, err := arg.String()
		if err == nil {
			err = rec.SetGeoJSON(name, s)
		}
		stage = tag(err)
	case "ints":
		if list, ok := ints(arg); ok {
			stage = tag(rec.SetInts(name, list))
		}
	case "doubles":
		if list, ok := doubles(arg); ok {
			stage = tag(rec.SetDoubles(name, list))
		}
	case "remove":
		stage = tag(rec.Remove(name))
	default:
		return nil, errors.New("wr: unknown how")
	}

	out := udf.NewList(3).Add(stage)
	if stage != "ok" {
		out.Add("skipped")
	} else {
		out.Add(tag(rec.Save()))
	}
	out.Add(rec.BinCount())

	return out, out.Err()
}

// Everything the record says of itself.
//
//udf:function meta
func meta(rec *udf.Record) (*udf.Map, error) {
	ttl := rec.TTL()
	if ttl <= 0 {
		ttl = -1
	}

	out := udf.NewMap(8).
		Put("exists", rec.Exists()).
		Put("generation", rec.Generation()).
		Put("ttl", ttl).
		Put("bins", rec.BinCount()).
		Put("sized", rec.Size() > 0).
		Put("set", rec.SetName())

	key, ok, err := rec.Key(keyBuf[:])
	if err != nil {
		return nil, err
	}
	if ok {
		out.Put("key", key)
	} else {
		out.Put("key", nil)
	}

	var buf [20]byte
	digest, err := rec.Digest(buf[:])
	if err != nil {
		return nil, err
	}
	out.Put("digest", len(digest))

	return out, out.Err()
}

// The kind of a value, the value passed back, and for a list or map its length.
//
//udf:function val
func val(v udf.Value) (*udf.List, error) {
	out := udf.NewList(3).Add(kind(v)).Add(v)
	if v.IsList() || v.IsMap() {
		out.Add(int64(v.Len))
	}
	return out, out.Err()
}

// Element i of the mixed list and the mixed map.
func mixed(i int64) any {
	switch i % 6 {
	case 0:
		return i
	case 1:
		return float64(i) + 0.5
	case 2:
		return i%4 == 2
	case 3:
		return "s" + strconv.FormatInt(i, 10)
	case 4:
		return []byte{byte(i)}
	}
	return nil
}

// n bytes, byte i being what f answers.
func fill(n int64, f func(i int) byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = f(i)
	}
	return b
}

// Builds a value with the builders, one call per element, and answers the value itself. A size the
// module does not build is refused before anything is; a builder that kept a refusal fails the call
// with its status.
//
//udf:function mk
func mk(what string, n int64) (udf.Value, error) {
	if n < 0 || n > mkMax {
		return udf.NilValue(), errors.New("mk: refused")
	}

	var built udf.Value
	var err error

	switch what {
	case "list_int":
		l := udf.NewList(int(n))
		for i := int64(0); i < n; i++ {
			l.Add(i)
		}
		built, err = l.Value(), l.Err()
	case "list_mixed":
		l := udf.NewList(int(n))
		for i := int64(0); i < n; i++ {
			l.Add(mixed(i))
		}
		built, err = l.Value(), l.Err()
	case "map_str":
		m := udf.NewMap(int(n))
		for i := int64(0); i < n; i++ {
			m.Put("k"+strconv.FormatInt(i, 10), i)
		}
		built, err = m.Value(), m.Err()
	case "map_int":
		m := udf.NewMap(int(n))
		for i := int64(0); i < n; i++ {
			m.Put(i, "v"+strconv.FormatInt(i, 10))
		}
		built, err = m.Value(), m.Err()
	case "map_mixed":
		m := udf.NewMap(int(n))
		for i := int64(0); i < n; i++ {
			m.Put("k"+strconv.FormatInt(i, 10), mixed(i))
		}
		built, err = m.Value(), m.Err()
	case "nested":
		// From the inside out, stopping at the first list the host would not nest.
		l := udf.NewList(1).Add(n)
		for i := int64(1); i < n && l.Err() == nil; i++ {
			inner := l
			l = udf.NewList(1).Add(inner)
			inner.Close() // the outer list keeps it
		}
		built, err = l.Value(), l.Err()
	case "string":
		return udf.StringBytesValue(fill(n, func(int) byte { return 'x' })), nil
	case "blob":
		return udf.BlobValue(fill(n, func(i int) byte { return byte(i) })), nil
	default:
		return udf.NilValue(), errors.New("mk: unknown what")
	}

	if err != nil {
		return udf.NilValue(), errors.New("mk: " + tag(err))
	}
	return built, nil
}

// element is an element or an entry a lookup answered, nil when it is not there.
func element(v udf.Value, ok bool) any {
	if ok {
		return v
	}
	return nil
}

// Walks the stored list or map in a bin with range, Index and the lookups.
//
//udf:function walk
func walk(rec *udf.Record, name string) (*udf.List, error) {
	v, ok, err := rec.Get(name)
	if err != nil {
		return only(tag(err))
	}
	if !ok {
		return only("none")
	}

	switch {
	case v.IsList():
		var sum, strings int64
		for e := range v.List() {
			if e.IsInt() {
				n, err := e.Int()
				if err != nil {
					return nil, err
				}
				sum += n
			}
			if e.IsString() {
				strings++
			}
		}

		first, hasFirst, err := v.Index(0)
		if err != nil {
			return nil, err
		}
		last, hasLast, err := v.Index(int(v.Len) - 1)
		if err != nil {
			return nil, err
		}

		out := udf.NewList(7).Add("list").Add(int64(v.Len)).Add(sum).Add(strings).
			Add(element(first, hasFirst)).
			Add(element(last, hasLast))

		switch _, hasPast, err := v.Index(int(v.Len)); {
		case err != nil:
			out.Add(tag(err))
		case hasPast:
			out.Add("some")
		default:
			out.Add("none")
		}

		return out, out.Err()

	case v.IsMap():
		var sum int64
		for _, e := range v.Map() {
			if e.IsInt() {
				n, err := e.Int()
				if err != nil {
					return nil, err
				}
				sum += n
			}
		}

		atK1, hasK1, err := v.Lookup("k1")
		if err != nil {
			return nil, err
		}
		at1, has1, err := v.LookupInt(1)
		if err != nil {
			return nil, err
		}

		out := udf.NewList(7).Add("map").Add(int64(v.Len)).Add(sum).Add(hasK1).Add(has1).
			Add(element(atK1, hasK1)).
			Add(element(at1, has1))

		return out, out.Err()
	}

	return only("wrong")
}

// A required argument, an optional one and the rest: a, b (nil when not given), how many more came,
// and the kind of each. The generated export fails a call with more than udf.MaxArgs arguments in
// total.
//
//udf:function ar
func ar(a int64, b *string, rest []udf.Value) (*udf.List, error) {
	out := udf.NewList(3 + len(rest)).Add(a)

	if b != nil {
		out.Add(*b)
	} else {
		out.Add(nil)
	}

	out.Add(len(rest))
	for _, v := range rest {
		out.Add(kind(v))
	}

	return out, out.Err()
}

// What a write is before and after Save, a delete, a TTL, and a failure after a save. A staging call
// that fails - the record cannot be written here - is the whole answer: its status, and nothing more
// is tried.
//
//udf:function life
func life(rec *udf.Record, what string) (*udf.List, error) {
	out := udf.NewList(3)

	switch what {
	case "staged":
		if err := rec.Set("t", 1); err != nil {
			return only(tag(err))
		}
		t, err := rec.IntOr("t", -1)
		if err != nil {
			return only(tag(err))
		}
		out.Add("some").Add(t).Add(rec.BinCount())
	case "save_twice":
		if err := rec.Set("t", 1); err != nil {
			return only(tag(err))
		}
		save1 := rec.Save()
		if err := rec.Set("t", 2); err != nil {
			return only(tag(err))
		}
		save2 := rec.Save()
		out.Add(tag(save1)).Add(tag(save2))
	case "delete":
		status := rec.Delete()
		out.Add(tag(status)).Add(rec.Exists())
	case "ttl":
		if err := rec.SetTTL(100); err != nil {
			return only(tag(err))
		}
		out.Add("ok").Add(tag(rec.Save()))
	case "fail_after_save":
		if err := rec.Set("t", 9); err != nil {
			return only(tag(err))
		}
		_ = rec.Save()
		return nil, errors.New("life: failing")
	default:
		return nil, errors.New("life: unknown what")
	}

	return out, out.Err()
}

// Sets the result with the Result call of each kind. It is in the raw form because a typed function
// returns its value and the generated export picks the call; here the call is the thing under test.
//
//udf:function res params="(kind: str)"
func res(args, argc int32) int32 {
	var a [1]udf.Value
	n := udf.Args(args, argc, a[:])
	if err := udf.Arity("res", len(a), argc); err != nil {
		return udf.FailErr(err)
	}
	which, err := udf.ArgString(a[:], n, 0, "res", "kind")
	if err != nil {
		return udf.FailErr(err)
	}

	switch which {
	case "nil":
		udf.ResultNil()
	case "int":
		udf.ResultInt(math.MinInt64)
	case "double":
		udf.ResultDouble(1e308)
	case "bool":
		udf.ResultBool(true)
	case "string":
		udf.ResultString("")
	case "blob":
		udf.ResultBlob(nil)
	case "geojson":
		udf.ResultGeoJSON(point)
	case "list":
		if err := udf.ResultList(udf.NewList(0)); err != nil {
			return udf.FailErr(err)
		}
	case "map":
		if err := udf.ResultMap(udf.NewMap(0)); err != nil {
			return udf.FailErr(err)
		}
	case "fail_long":
		return udf.Fail(string(fill(600, func(int) byte { return 'x' })))
	default:
		return udf.Fail("res: unknown kind")
	}

	return 0
}

// The byte length of a string, the string passed back, and its first byte (-1 for the empty string).
//
//udf:function txt
func txt(s string) (*udf.List, error) {
	first := int64(-1)
	if len(s) > 0 {
		first = int64(s[0])
	}

	out := udf.NewList(3).Add(len(s)).Add(s).Add(first)
	return out, out.Err()
}

// Compares the stored values of two bins with CmpStored, which answers an ordering or an error: a
// scalar, and any pair it cannot compare, is the error's status.
//
//udf:function cmp
func cmpBins(rec *udf.Record, a string, b string) (*udf.List, error) {
	x, hasX, err := rec.Get(a)
	if err != nil {
		return only(tag(err))
	}
	y, hasY, err := rec.Get(b)
	if err != nil {
		return only(tag(err))
	}
	if !hasX || !hasY {
		return only("none")
	}

	return orAnswer(udf.CmpStored(x, y))
}

// Agg counts and adds up the integer bin n of every record of a stream.
type Agg struct {
	count int64
	sum   int64
}

//udf:stream agg
var agg Agg

// Record takes one record of the stream: one with no bin n, or with an n that is not an integer, is
// not counted.
func (s *Agg) Record(rec *udf.Record) {
	if n, ok, err := rec.Int("n"); err == nil && ok {
		s.count++
		s.sum += n
	}
}

// End answers [count, sum].
func (s *Agg) End() (*udf.List, error) {
	out := udf.NewList(2).Add(s.count).Add(s.sum)
	return out, out.Err()
}

// A list of the names a walk gives.
func nameList(names func(yield func(string) bool)) *udf.List {
	out := udf.NewList(8)
	for name := range names {
		out.Add(name)
	}
	return out
}

// Everything an xdr-merge handler can read of its two records, returned as the bins of the record. A
// side where n is not an integer, or s not a string, fails the handler: the Pair reader answers the
// error. Sum has no "nothing" answer - it is 0 when neither side has the bin - so the sum bin is
// always there.
//
//udf:on xdr-merge merge_api
func mergeAPI(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	pair := udf.PairOf(current, incoming)
	out := udf.NewMap(20)

	n, err := pair.Int("n")
	if err != nil {
		return udf.Resolution{}, err
	}
	if v, ok := udf.Max(n); ok {
		out.Put("max", v)
	}
	if v, ok := udf.Min(n); ok {
		out.Put("min", v)
	}
	if v, ok := udf.Sum(n); ok {
		out.Put("sum", v)
	}

	s, err := pair.String("s")
	if err != nil {
		return udf.Resolution{}, err
	}
	if v, ok := s.Newer(); ok {
		out.Put("newer_s", v)
	}
	if v, ok := s.Older(); ok {
		out.Put("older_s", v)
	}
	if v, ok := s.CurrentOrIncoming(); ok {
		out.Put("c_or_i", v)
	}
	if v, ok := s.IncomingOrCurrent(); ok {
		out.Put("i_or_c", v)
	}

	out.Put("i_newer", pair.IncomingIsNewer("s"))

	out.Put("c_names", nameList(current.BinNames()))
	out.Put("i_names", nameList(incoming.BinNames()))
	out.Put("c_del", nameList(current.DeletedBins()))
	out.Put("i_del", nameList(incoming.DeletedBins()))
	out.Put("lost", nameList(incoming.LostBins()))

	out.Put("c_meta", udf.NewList(6).
		Add(current.Generation()).
		Add(current.LastUpdateTime()).
		Add(current.TTL()).
		Add(current.SourceID()).
		Add(current.IsTombstone()).
		Add(current.UsesBinConvergence()))
	out.Put("i_meta", udf.NewList(6).
		Add(incoming.Generation()).
		Add(incoming.LastUpdateTime()).
		Add(incoming.TTL()).
		Add(incoming.SourceID()).
		Add(incoming.IsDelete()).
		Add(incoming.IsPartial()))

	out.Put("blut", udf.NewList(2).
		Add(current.BinLastUpdateTime("s")).
		Add(incoming.BinLastUpdateTime("s")))
	out.Put("bsrc", udf.NewList(2).
		Add(current.BinSourceID("s")).
		Add(incoming.BinSourceID("s")))

	return udf.Resolve(out), out.Err()
}

func main() {}
