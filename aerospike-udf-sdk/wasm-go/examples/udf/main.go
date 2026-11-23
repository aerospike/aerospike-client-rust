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

// The Go counterpart of the Rust SDK's test fixture (aerospike-udf-sdk/wasm-rust/examples/udf): one function
// per corner of the record API that the Rust SDK has and the client tests check, so that
// tests/src/udf_go.rs can hold the Go SDK to the same surface. Nothing here is an example to copy; it is
// the SDK's own coverage.
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go/cmd/udfgen

import (
	"errors"
	"math"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go"
)

const geo = `{"type":"Point","coordinates":[1.0,2.0]}`

// The sum of the integers in list bin l, walked with range; any other element fails the call.
//
//udf:function sum_list
func sumList(rec *udf.Record) (int64, error) {
	var total int64

	list, _, err := rec.List("l")
	if err != nil {
		return 0, err
	}

	for v := range list.List() {
		n, err := v.Int()
		if err != nil {
			return 0, errors.New("bin `l` holds something that is not an integer")
		}
		total += n
	}

	return total, nil
}

// Everything the record knows about itself, as a map.
//
//udf:function describe
func describe(rec *udf.Record) (*udf.Map, error) {
	out := udf.NewMap(8).
		Put("generation", rec.Generation()).
		Put("bins", rec.BinCount()).
		Put("size", rec.Size()).
		Put("lut", rec.LastUpdateTime()).
		Put("ttl", rec.TTL())

	if set := rec.SetName(); set != "" {
		out.Put("set", set)
	} else {
		out.Put("set", nil)
	}

	return out, out.Err()
}

// A scalar bin that must be there, through whichever record is in hand: Bin answers the error that
// names a missing bin, and the value's own accessor the one for a bin of another kind.
func mustInt(rec udf.RecordReader, name string) (int64, error) {
	v, err := rec.Bin(name)
	if err != nil {
		return 0, err
	}
	return v.Int()
}

// The typed readers over scalar bins, and a map ranged over entry by entry.
//
//udf:function typed
func typed(rec *udf.Record) (*udf.Map, error) {
	i, err := mustInt(rec, "i")
	if err != nil {
		return nil, err
	}
	d, ok, err := rec.Double("d")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("bin `d` is absent")
	}
	s, ok, err := rec.String("s")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("bin `s` is absent")
	}
	b, ok, err := rec.Bool("b")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("bin `b` is absent")
	}

	m, _, err := rec.Map("m")
	if err != nil {
		return nil, err
	}

	var sum int64
	keys := udf.NewList(4)

	for k, v := range m.Map() {
		if n, err := v.Int(); err == nil {
			sum += n
		}
		keys.Add(k)
	}

	out := udf.NewMap(8).Put("i", i).Put("d", d).Put("s", s).Put("b", b).Put("msum", sum).Put("keys", keys)

	return out, out.Err()
}

// The writes beyond the plain scalars: packed lists, a bool, a nil and a GeoJSON.
//
//udf:function write_lists
func writeLists(rec *udf.Record) (int64, error) {
	if err := rec.SetInts("w", []int64{5, -1, 1 << 40}); err != nil {
		return 0, err
	}
	if err := rec.SetDoubles("wd", []float64{0.5, -4.0}); err != nil {
		return 0, err
	}
	if err := rec.Set("flag", true); err != nil {
		return 0, err
	}
	if err := rec.Set("gone", nil); err != nil {
		return 0, err
	}
	if err := rec.Set("geo", udf.GeoJSONValue(geo)); err != nil {
		return 0, err
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return 1, nil
}

// [doc[index], <geojson>, {1: "one", "first": doc[0]}]: stored values into the builders by position, a
// GeoJSON that stays a GeoJSON, and a map with an integer key.
//
//udf:function relist
func relist(rec *udf.Record, index int64) (*udf.List, error) {
	var first, picked udf.Value
	found := false
	i := int64(0)

	doc, _, err := rec.List("doc")
	if err != nil {
		return nil, err
	}

	for v := range doc.List() {
		if i == 0 {
			first = v
		}
		if i == index {
			picked = v
			found = true
			break
		}
		i++
	}

	if !found {
		return nil, errors.New("relist: doc has no such element")
	}

	inner := udf.NewMap(2).Put(1, "one").Put("first", first)
	out := udf.NewList(3).Add(picked).AddGeoJSON(geo).Add(inner)

	return out, out.Err()
}

// Writes a GeoJSON bin back as the kind it was read as, and reports the document's length.
//
//udf:function geo_roundtrip
func geoRoundtrip(rec *udf.Record, name string) (int64, error) {
	v, ok, err := rec.Get(name)
	if err != nil {
		return 0, err
	}

	if !ok || !v.IsGeoJSON() {
		return 0, errors.New("that bin does not hold geojson")
	}

	if err := rec.Set(name, v); err != nil {
		return 0, err
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return int64(v.Len), nil
}

// A GeoJSON of the guest's as the result.
//
//udf:function geo_result
func geoResult() udf.Value {
	return udf.GeoJSONValue(geo)
}

// Sets the record's TTL and saves.
//
//udf:function touch_ttl
func touchTTL(rec *udf.Record, seconds int64) (int64, error) {
	if err := rec.SetTTL(int32(seconds)); err != nil {
		return 0, err
	}
	if err := rec.Set("touched", 1); err != nil {
		return 0, err
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return 1, nil
}

// Deletes the record.
//
//udf:function remove_me
func removeMe(rec *udf.Record) (int64, error) {
	if err := rec.Delete(); err != nil {
		return 0, err
	}
	return 1, nil
}

// Saves with nothing staged, and reports what that did: whether the record exists afterwards.
//
// It is here so that a test can pin down what Save means when there is nothing to save, on a record that
// is there and on one that is not.
//
//udf:function save_nothing
func saveNothing(rec *udf.Record) (bool, error) {
	if err := rec.Save(); err != nil {
		return false, err
	}
	return rec.Exists(), nil
}

// Deletes the record, staged writes and all. Answers what Exists says straight afterwards.
//
//udf:function delete_it
func deleteIt(rec *udf.Record) (bool, error) {
	if err := rec.Set("n", 99); err != nil {
		return false, err
	}
	if err := rec.Delete(); err != nil {
		return false, err
	}
	return rec.Exists(), nil
}

// Removes a bin by name and saves. Answers how many bins are left.
//
//udf:function drop_bin
func dropBin(rec *udf.Record, name string) (int64, error) {
	if err := rec.Remove(name); err != nil {
		return 0, err
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return rec.BinCount(), nil
}

// Sets a bin to nil and saves, which removes it: a bin cannot hold nil.
//
//udf:function nil_bin
func nilBin(rec *udf.Record, name string) (int64, error) {
	if err := rec.Set(name, nil); err != nil {
		return 0, err
	}
	if err := rec.Save(); err != nil {
		return 0, err
	}
	return rec.BinCount(), nil
}

// One element of a list bin by its index, and whether a map the call builds knows its keys.
//
//udf:function index_probe
func indexProbe(rec *udf.Record, name string, index int64) (*udf.Map, error) {
	list, err := rec.Bin(name)
	if err != nil {
		return nil, err
	}
	if !list.IsList() {
		return nil, errors.New("that bin does not hold a list")
	}

	out := udf.NewMap(4).Put("len", int64(list.Len))

	// An index too large for this target's int is past the end of any list it could hold.
	if index < 0 || index > math.MaxInt32 {
		out.Put("at", "absent")
	} else if v, ok, err := list.Index(int(index)); err != nil {
		return nil, err
	} else if ok {
		out.Put("at", v)
	} else {
		out.Put("at", "absent")
	}

	out.Put(7, "seven")

	knows := out.Contains("len") && out.Contains(7)
	invents := out.Contains("nope") || out.Contains(8)

	out.Put("knows", knows).Put("invents", invents)

	return out, out.Err()
}

// Build count maps, each holding a list, nested in one outer list and closed as soon as they are in,
// then walk every list of list bin nested with a range loop of its own. Answers [elements, sum].
//
// The call holds a few handles at a time however large count is: what bounds it is its memory, not a
// number of handles.
//
//udf:function churn
func churn(rec *udf.Record, count int64) (*udf.List, error) {
	out := udf.NewList(0)
	defer out.Close()

	for i := int64(0); i < count; i++ {
		inner := udf.NewList(2).Add(i).Add(1)
		entry := udf.NewMap(1).Put("l", inner)
		inner.Close() // entry keeps it
		out.Add(entry)
		entry.Close() // out keeps it
	}
	if err := out.Err(); err != nil {
		return nil, err
	}

	nested, _, err := rec.List("nested")
	if err != nil {
		return nil, err
	}
	var sum int64
	for row := range nested.List() {
		for cell := range row.List() {
			n, err := cell.Int()
			if err != nil {
				return nil, err
			}
			sum += n
		}
	}

	answer := udf.NewList(2).Add(out.Len()).Add(sum)
	return answer, answer.Err()
}

func main() {}
