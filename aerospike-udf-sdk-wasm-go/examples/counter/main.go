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

// The smallest CRDT in Go: a grow-only counter, merged across data centres by the xdr-merge event. The
// same module as aerospike-udf-sdk-wasm/examples/counter.
//
// One bin, counts, holds a map of site -> count. Each data centre adds only to its own entry
// (increment("dc1", 3)), and the counter's value is the sum of every entry (value()). When a write from one
// site arrives at the other, merge_counter keeps, for every site, the larger of the two counts. A site's
// count only ever grows, so the larger one is always the later one: the merge needs no clock, no LUT and
// no writer id, and it is commutative, idempotent and associative by construction, which the server's dry
// run checks on real bytes.
//
//	event-set:event=xdr-merge;namespace=test;set=counters;module=counter;function=merge_counter;on-failure=abandon
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	"errors"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

const siteMax = 32

var counts = udf.NewBin("counts")

// The per-site counts gathered from one or both sides: for every site, the larger count seen.
type table map[string]int64

func (t table) offer(site string, count int64) {
	if count > t[site] {
		t[site] = count
	}
}

// Every entry of a stored counts map.
func (t table) offerAll(stored udf.Value) error {
	for k, v := range stored.Map() {
		site, err := k.Str()
		if err != nil {
			return err
		}

		count, err := v.Int()
		if err != nil {
			return err
		}

		t.offer(site, count)
	}

	return nil
}

func (t table) total() int64 {
	var sum int64

	for _, c := range t {
		sum += c
	}

	return sum
}

// The counts as a built map, for a bin or a resolution.
func (t table) toMap() *udf.Map {
	m := udf.NewMap(len(t))

	for site, c := range t {
		m.Put(site, c)
	}

	return m
}

// The merge: for every site, the larger of the two counts. Every other bin is the incoming one's.
//
//udf:on xdr-merge merge_counter
func mergeCounter(current, incoming *udf.EventRecord, _ *udf.Conflict) (udf.Resolution, error) {
	t := table{}

	for _, side := range []*udf.EventRecord{current, incoming} {
		if stored, ok := side.Get("counts"); ok {
			if err := t.offerAll(stored); err != nil {
				return udf.Resolution{}, err
			}
		}
	}

	out := udf.NewMap(2).Put("counts", t.toMap())

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}

// The record's counts.
func stored() (table, error) {
	t := table{}

	if v, ok := counts.Get(); ok {
		if err := t.offerAll(v); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// Adds n to this site's count. Returns the counter's value.
//
//udf:function increment
func increment(site string, n int64) (int64, error) {
	// The Rust module bounds the site name with a Str<32>; the same limit, as a check.
	if len(site) > siteMax {
		return 0, errors.New("increment: the site name is too long")
	}

	t, err := stored()
	if err != nil {
		return 0, err
	}

	t[site] += n

	if err := counts.SetMap(t.toMap()); err != nil {
		return 0, err
	}

	if err := udf.Commit(); err != nil {
		return 0, err
	}

	return t.total(), nil
}

// The counter's value: the sum of every site's count.
//
//udf:function value
func value() (int64, error) {
	t, err := stored()
	if err != nil {
		return 0, err
	}

	return t.total(), nil
}

func main() {}
