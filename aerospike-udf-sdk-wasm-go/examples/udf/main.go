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

// The Go counterpart of the Rust SDK's test fixture (aerospike-udf-sdk-wasm/examples/udf): one function
// per corner of the record API that the Rust SDK has and the client tests check, so that
// tests/src/udf_go.rs can hold the Go SDK to the same surface. Nothing here is an example to copy; it is
// the SDK's own coverage.
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	"errors"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

const geo = `{"type":"Point","coordinates":[1.0,2.0]}`

var (
	l   = udf.NewBin("l")
	m   = udf.NewBin("m")
	doc = udf.NewBin("doc")
)

// The sum of the integers in list bin l, walked with range; any other element fails the call.
//
//udf:function sum_list
func sumList() (int64, error) {
	var total int64

	for v := range l.List() {
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
func describe() (*udf.Map, error) {
	out := udf.NewMap(8).
		Put("generation", udf.Generation()).
		Put("bins", udf.BinCount()).
		Put("size", udf.RecordSize()).
		Put("lut", udf.LastUpdateTime()).
		Put("ttl", udf.TTL())

	if set := udf.SetName(); set != "" {
		out.Put("set", set)
	} else {
		out.Put("set", nil)
	}

	return out, out.Err()
}

// The typed readers over scalar bins, and a map ranged over entry by entry.
//
//udf:function typed
func typed() (*udf.Map, error) {
	i, err := udf.NewBin("i").Int()
	if err != nil {
		return nil, err
	}
	d, err := udf.NewBin("d").Float64()
	if err != nil {
		return nil, err
	}
	s, err := udf.NewBin("s").Str()
	if err != nil {
		return nil, err
	}
	b, err := udf.NewBin("b").Bool()
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

// The writes the typed natives do not cover: packed lists, a bool, a nil and a GeoJSON.
//
//udf:function write_lists
func writeLists() (int64, error) {
	if err := udf.NewBin("w").SetInts([]int64{5, -1, 1 << 40}); err != nil {
		return 0, err
	}
	if err := udf.NewBin("wd").SetFloat64s([]float64{0.5, -4.0}); err != nil {
		return 0, err
	}
	if err := udf.NewBin("flag").SetBool(true); err != nil {
		return 0, err
	}
	if err := udf.NewBin("gone").SetNull(); err != nil {
		return 0, err
	}
	if err := udf.NewBin("geo").SetGeoJSON(geo); err != nil {
		return 0, err
	}
	if err := udf.Commit(); err != nil {
		return 0, err
	}
	return 1, nil
}

// [doc[index], <geojson>, {1: "one", "first": doc[0]}]: stored values into the builders by position, a
// GeoJSON that stays a GeoJSON, and a map with an integer key.
//
//udf:function relist
func relist(index int64) (*udf.List, error) {
	var first, picked udf.Value
	found := false
	i := int64(0)

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

	inner := udf.NewMap(2).PutI(1, "one").Put("first", first)
	out := udf.NewList(3).Add(picked).AddGeoJSON(geo).Add(inner)

	return out, out.Err()
}

// Writes a GeoJSON bin back as the kind it was read as, and reports the document's length.
//
//udf:function geo_roundtrip
func geoRoundtrip(name string) (int64, error) {
	bin := udf.NewBin(name)
	v, ok := bin.Get()

	if !ok || v.Kind != udf.KindGeoJSON {
		return 0, errors.New("that bin does not hold geojson")
	}

	if err := bin.SetValue(v); err != nil {
		return 0, err
	}
	if err := udf.Commit(); err != nil {
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

// Sets the record's TTL and commits.
//
//udf:function touch_ttl
func touchTTL(seconds int64) (int64, error) {
	if err := udf.SetTTL(int32(seconds)); err != nil {
		return 0, err
	}
	if err := udf.NewBin("touched").SetInt(1); err != nil {
		return 0, err
	}
	if err := udf.Commit(); err != nil {
		return 0, err
	}
	return 1, nil
}

// Deletes the record.
//
//udf:function remove_me
func removeMe() (int64, error) {
	if err := udf.Delete(); err != nil {
		return 0, err
	}
	return 1, nil
}

func main() {}
