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
	"strconv"
	"unsafe"
)

//==========================================================
// Collections built on the host: the Go face of the Rust SDK's List and Map. Put and Add take a value of
// any supported Go type and chain; the first refusal is kept and answered by Err, the way strings.Builder
// and bufio.Writer keep theirs, and a generated export checks it when a *Map or *List is returned.
//

// Map is a map built on the host, to be written to a bin, returned as the result, nested in another
// collection, or handed back from an event handler as the record. Make one with NewMap; it lives until
// the call ends.
//
// Put and its relatives return the map, so they chain, and none of them returns an error: the first
// refusal is kept and answered by Err, the way strings.Builder keeps its error. Record.Set,
// ResultMap, Resolve and the generated exports check it when they are given the map; check it
// yourself where nothing else will.
//
// The host keeps the map ordered by key, so it reaches a client in key order whatever order the
// entries were put in.
//
//	//udf:function describe
//	func describe(rec *udf.Record) (*udf.Map, error) {
//		out := udf.NewMap(4).
//			Put("generation", rec.Generation()).
//			Put("bins", rec.BinCount()).
//			Put("tags", udf.NewList(2).Add("a").Add("b")).
//			Put(7, "seven") // an integer key
//
//		return out, out.Err()
//	}
type Map struct {
	handle int32
	err    error
}

// List is a list built on the host, to be written to a bin, returned as the result or nested in
// another collection. Make one with NewList; it lives until the call ends. Add and its relatives
// chain, and the first refusal is kept and answered by Err, as with Map.
//
//	tags := udf.NewList(4).Add("new").Add(42).Add(1.5)
//	if err := rec.Set("tags", tags); err != nil { // Set answers what the list kept, too
//		return err
//	}
type List struct {
	handle int32
	err    error
}

// NewMap starts an empty map. capacity is a hint of how many entries are coming, not a limit: the map
// grows past it, 0 is fine, and the host caps what it reserves at 1024.
//
// It never returns nil. When the host cannot make the map - the call holds its 256 handles, or has
// spent what it may build on the host - the map returned has kept that error, an *Error with Status
// Refused: Err answers it, every Put on it fails, and Len is -1.
//
// A built map or list holds one of the call's 256 handles until the call ends, whatever becomes of
// it; its entries hold none, however many there are and whatever their kinds.
func NewMap(capacity int) *Map {
	h := mapNew(int32(capacity))
	if h <= 0 {
		return &Map{err: errorf(Refused, "could not build a map: the call holds its 256 handles, or has spent what it may build")}
	}
	return &Map{handle: h}
}

// NewList starts an empty list. capacity is a hint of how many elements are coming, not a limit: the
// list grows past it, 0 is fine, and the host caps what it reserves at 1024.
//
// It never returns nil. When the host cannot make the list the one returned has kept that error, as
// NewMap's does.
func NewList(capacity int) *List {
	h := listNew(int32(capacity))
	if h <= 0 {
		return &List{err: errorf(Refused, "could not build a list: the call holds its 256 handles, or has spent what it may build")}
	}
	return &List{handle: h}
}

// Err is the first refusal the map kept, nil when the map was made and every put went in. It is an
// *Error whose message names the key that failed and whose Status says why: Refused when the host
// would not take the entry - the call has spent what it may build, or the value would nest more than
// 64 deep - TypeMismatch for a Go type or a Value that cannot be put, NotFound for a Value of
// KindAbsent. Later puts are still attempted after a refusal, so a map with an error is partly built
// and not to be used.
func (m *Map) Err() error { return m.err }

// Err is the first refusal the list kept, nil when the list was made and every add went in. Later
// adds are still attempted after a refusal, so a list with an error is partly built and not to be
// used.
func (l *List) Err() error { return l.err }

// Value is the map as a Value, of KindHandle, for a batch (PutEntries, AddValues) or for Result. It
// does not carry what Err kept, so check Err before handing it on. Put, Add and Record.Set take the
// *Map itself.
func (m *Map) Value() Value { return handleValue(m.handle) }

// Value is the list as a Value, of KindHandle, for a batch (PutEntries, AddValues) or for Result. It
// does not carry what Err kept, so check Err before handing it on.
func (l *List) Value() Value { return handleValue(l.handle) }

// Len is how many entries the map holds, asked of the host in one import; -1 when it could not be
// built, as after a failed NewMap.
func (m *Map) Len() int { return int(collSize(m.handle)) }

// Len is how many elements the list holds, asked of the host in one import; -1 when it could not be
// built, as after a failed NewList.
func (l *List) Len() int { return int(collSize(l.handle)) }

// mapKey is a Go value as a map key: a string, or an integer. ok is false for any other type.
func mapKey(key any) (text string, number int64, isInt bool, ok bool) {
	switch k := key.(type) {
	case string:
		return k, 0, false, true
	case int64:
		return "", k, true, true
	case int:
		return "", int64(k), true, true
	case int32:
		return "", int64(k), true, true
	}
	return "", 0, false, false
}

// keyValue is a map key as the Value the batch import takes: a string by the address of its bytes, of
// any length, or an integer.
func keyValue(text string, number int64, isInt bool) Value {
	if isInt {
		return IntValue(number)
	}
	return StringValue(text)
}

// Contains reports whether the map holds key, a string or an integer (int, int32, int64), in one
// import: the host answers from the map as built, nothing is packed. It is false for a key of any
// other type, and for a map that could not be built.
func (m *Map) Contains(key any) bool {
	text, number, isInt, ok := mapKey(key)
	if !ok {
		return false
	}
	if isInt {
		return mapHasI(m.handle, number) == 1
	}
	return mapHas(m.handle, strptr(text), int32(len(text))) == 1
}

func (m *Map) fail(rc int32, key string) *Map {
	if rc != OK && m.err == nil {
		m.err = errorf(rc, "could not put ", key, " (", strconv.Itoa(int(rc)), ")")
	}
	return m
}

func (l *List) fail(rc int32) *List {
	if rc != OK && l.err == nil {
		l.err = errorf(rc, "could not add to the list (", strconv.Itoa(int(rc)), ")")
	}
	return l
}

// valueOf is any supported Go value as a Value: a scalar by value, a string or blob by the address of its
// bytes, a built collection by its handle. ok is false for an unsupported type.
func valueOf(v any) (Value, bool) {
	switch x := v.(type) {
	case nil:
		return NilValue(), true
	case int64:
		return IntValue(x), true
	case int:
		return IntValue(int64(x)), true
	case int32:
		return IntValue(int64(x)), true
	case float64:
		return DoubleValue(x), true
	case float32:
		return DoubleValue(float64(x)), true
	case bool:
		return BoolValue(x), true
	case string:
		return StringValue(x), true
	case []byte:
		return BlobValue(x), true
	case Value:
		return x, true
	case *Map:
		return x.Value(), true
	case *List:
		return x.Value(), true
	}
	return Value{Kind: KindAbsent}, false
}

// Put puts a value under a key and returns the map. The key is a string or an integer (int, int32,
// int64); the value an int, int32, int64, float32, float64, bool, string, []byte (a blob), Value,
// *Map, *List, or nil for a nil entry (in a record answer, "no such bin"). A key or value of any
// other Go type is a refusal, kept for Err, and so is an inner *Map or *List that had kept one.
//
// A string or blob is copied by the host during the call. A Value read from a record goes in from its
// stored bytes, without being decoded, and keeps its kind. The host refuses a collection that would
// come to contain itself, or to nest more than 64 deep.
//
// Each put is one import and takes none of the 256 handles a call may hold, whatever the kinds of the
// key and the value, so a loop may put as many entries as the call may build. A key is put whole,
// whatever its length. PutEntries puts many entries in one import.
func (m *Map) Put(key any, v any) *Map {
	text, number, isInt, ok := mapKey(key)
	if !ok {
		if m.err == nil {
			m.err = errorf(TypeMismatch, "could not put an entry: the key is neither a string nor an integer")
		}
		return m
	}
	if isInt {
		text = strconv.FormatInt(number, 10)
	}

	val, ok := valueOf(v)
	if !ok {
		if m.err == nil {
			m.err = errorf(TypeMismatch, "could not put ", text, ": unsupported Go type")
		}
		return m
	}
	if c, is := v.(*Map); is && c.err != nil && m.err == nil {
		m.err = c.err
	}
	if c, is := v.(*List); is && c.err != nil && m.err == nil {
		m.err = c.err
	}

	return m.fail(mapPutValue(m.handle, keyValue(text, number, isInt), val), text)
}

// PutAll puts every entry of a stored map, by position: neither the keys nor the values are decoded
// on the way. A value that is absent or nil has no entries and nothing is put; a value of any other
// kind than a map is a refusal, kept for Err with Status TypeMismatch, and so is an entry under a key
// that is neither a string nor an integer. Returns the map.
func (m *Map) PutAll(stored Value) *Map {
	if stored.IsNull() {
		return m
	}
	if stored.Kind != KindMap {
		if m.err == nil {
			m.err = stored.kindError("a map")
		}
		return m
	}
	for k, v := range stored.Map() {
		if k.Kind != KindInt && k.Kind != KindStr {
			if m.err == nil {
				m.err = errorf(TypeMismatch, "could not put an entry: the key is neither a string nor an integer")
			}
			continue
		}
		if rc := mapPutValue(m.handle, k, v); rc != OK && m.err == nil {
			m.err = errorf(rc, "could not put an entry of the stored map (", strconv.Itoa(int(rc)), ")")
		}
	}
	return m
}

// PutEntries puts a batch of entries in one import and returns the map. pairs is flat, key then
// value: pairs[0] is the first key and pairs[1] its value. A trailing key with no value is ignored.
//
// Keys and values are Values of any kind the host can take: scalars from IntValue, DoubleValue and
// BoolValue, guest bytes by address from StringValue, BlobValue and GeoJSONValue, stored values by
// position, built collections as their Value. The bytes behind a guest value have to stay alive until
// the call returns. It takes none of the call's handles, however many entries there are.
//
// When the host cannot take an entry it stops there: the entries before it are in, and Err names the
// index of the one that failed, with Status Refused.
//
//	pairs := []udf.Value{
//		udf.StringValue("x"), udf.DoubleValue(1.5),
//		udf.StringValue("y"), udf.DoubleValue(2.5),
//	}
//	point := udf.NewMap(2).PutEntries(pairs)
func (m *Map) PutEntries(pairs []Value) *Map {
	if len(pairs) < 2 {
		return m
	}
	put := mapPutEntries(m.handle, unsafe.Pointer(&pairs[0]), int32((len(pairs)/2)*32))
	if put < 0 {
		if m.err == nil {
			m.err = statusError(put, "could not put the entries")
		}
	} else if int(put) != len(pairs)/2 && m.err == nil {
		m.err = errorf(Refused, "could not put entry ", strconv.Itoa(int(put)))
	}
	return m
}

// AddValues appends a batch of values in one import and returns the list. The values are of the kinds
// PutEntries takes, with the same rule on the bytes behind a guest value, and it takes none of the
// call's handles. When the host cannot take a value it stops there: the values before it are in, and
// Err names the index of the one that failed, with Status Refused.
//
//	vals := make([]udf.Value, 0, len(scores))
//	for _, s := range scores {
//		vals = append(vals, udf.DoubleValue(s))
//	}
//	list := udf.NewList(len(vals)).AddValues(vals)
func (l *List) AddValues(vals []Value) *List {
	if len(vals) == 0 {
		return l
	}
	added := listPushValues(l.handle, unsafe.Pointer(&vals[0]), int32(len(vals)*16))
	if added < 0 {
		if l.err == nil {
			l.err = statusError(added, "could not add the values")
		}
	} else if int(added) != len(vals) && l.err == nil {
		l.err = errorf(Refused, "could not add value ", strconv.Itoa(int(added)))
	}
	return l
}

// PutGeoJSON puts a GeoJSON document under a string key, kept as that kind; Put with a string would
// make it a string. The host copies the text and does not check it. Returns the map.
func (m *Map) PutGeoJSON(key, doc string) *Map {
	return m.fail(mapPutValue(m.handle, StringValue(key), GeoJSONValue(doc)), key)
}

// Add appends a value and returns the list: an int, int32, int64, float32, float64, bool, string,
// []byte (a blob), Value, *Map, *List, or nil for a nil element, as Map.Put takes them. A value of
// any other Go type is a refusal, kept for Err.
//
// Each add is one import and takes none of the 256 handles a call may hold, whatever the kind, so a
// loop may add as many elements as the call may build. AddValues adds many in one import. The
// refusal Err keeps has Status Refused when the host would not take the element, TypeMismatch for a
// Go type or a Value that cannot be added, NotFound for a Value of KindAbsent.
func (l *List) Add(v any) *List {
	val, ok := valueOf(v)
	if !ok {
		if l.err == nil {
			l.err = errorf(TypeMismatch, "could not add to the list: unsupported Go type")
		}
		return l
	}
	if c, is := v.(*Map); is && c.err != nil && l.err == nil {
		l.err = c.err
	}
	if c, is := v.(*List); is && c.err != nil && l.err == nil {
		l.err = c.err
	}
	return l.fail(listPushValue(l.handle, val))
}

// AddGeoJSON appends a GeoJSON document, kept as that kind; Add with a string would make it a string.
// The host copies the text and does not check it. Returns the list.
func (l *List) AddGeoJSON(doc string) *List {
	return l.fail(listPushValue(l.handle, GeoJSONValue(doc)))
}

// AddAll appends every element of a stored list, in order and by position: the values are not decoded
// on the way. A value that is absent or nil has no elements and nothing is added; a value of any
// other kind than a list is a refusal, kept for Err with Status TypeMismatch. Returns the list.
func (l *List) AddAll(stored Value) *List {
	if stored.IsNull() {
		return l
	}
	if stored.Kind != KindList {
		if l.err == nil {
			l.err = stored.kindError("a list")
		}
		return l
	}
	for v := range stored.List() {
		l.Add(v)
	}
	return l
}

// Contains reports whether a stored list holds the string text, of any length. It walks the list and
// compares each string element, so it costs the length of the list; elements that are not strings
// never match (see StringIs). False when list is not a list.
//
//	if !udf.Contains(older, text) {
//		tags.Add(tag) // the union, without duplicates
//	}
func Contains(list Value, text string) bool {
	for v := range list.List() {
		if StringIs(v, text) {
			return true
		}
	}
	return false
}

//==========================================================
// Under the builders: integer keys, and the lookups in a stored map.
//

func guestPtr(v Value) unsafe.Pointer { return unsafe.Pointer(uintptr(v.Payload)) }

// guestBytes is the bytes a value of the guest's own was made over.
func guestBytes(v Value) []byte {
	if v.Len == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(guestPtr(v)), int(v.Len))
}

// getI looks an integer key up in the map at a value's position.
func getI(m Value, key int64, out *Value) int32 {
	return collGetI(m.Src(), m.At(), key, unsafe.Pointer(out), 16)
}
