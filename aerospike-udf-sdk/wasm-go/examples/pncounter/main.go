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

// A counter that can go down, in Go: the PN-Counter, two grow-only counters glued together, merged across
// data centres by the xdr-merge event. The same module as aerospike-udf-sdk/wasm-rust/examples/pncounter.
//
// The grow-only counter cannot subtract: a decrement would shrink a site's entry, and the per-site maximum
// would throw the shrink away. So this module never subtracts. The record keeps two maps of site -> amount,
// inc and dec, and both only ever grow. A site is the cluster's XDR src-id as a decimal string: on the
// cluster with src-id 1, increment(3) adds to inc["1"], decrement(1) adds to dec["1"], and the counter's
// value is the sum of inc minus the sum of dec. The merge is the grow-only
// counter's, run over each map on its own: for every site, the larger amount.
//
// What it cannot do is hold a floor: two sites that each see a stock of one and each take one merge to
// minus one. A bound is not a property a merge can keep.
//
//	event-set:event=xdr-merge;namespace=test;set=stock;module=pncounter;function=merge_pncounter;on-failure=abandon
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go/cmd/udfgen

import (
	"errors"
	"strconv"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go"
)

// The per-site amounts of one map, gathered from one or both sides: for every site, the larger amount seen.
type table map[string]int64

func (t table) offer(site string, count int64) {
	if count > t[site] {
		t[site] = count
	}
}

// Every entry of a stored map.
func (t table) offerAll(stored udf.Value) error {
	for k, v := range stored.Map() {
		site, err := k.String()
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

// The amounts as a built map, for a bin or a resolution.
func (t table) toMap() *udf.Map {
	m := udf.NewMap(len(t))

	for site, c := range t {
		m.Put(site, c)
	}

	return m
}

// The merge: for every site of each map, the larger amount. Every other bin is the incoming one's.
//
//udf:on xdr-merge merge_pncounter
func mergePnCounter(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	out := udf.NewMap(2)

	for _, bin := range []string{"inc", "dec"} {
		t := table{}

		for _, side := range []*udf.EventRecord{current, incoming} {
			// A side without the bin has no entries to offer; a bin that is not a map is an error.
			stored, _, err := side.Map(bin)
			if err != nil {
				return udf.Resolution{}, err
			}

			if err := t.offerAll(stored); err != nil {
				return udf.Resolution{}, err
			}
		}

		out.Put(bin, t.toMap())
	}

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}

// One of the record's maps, by its bin's name.
func stored(rec *udf.Record, bin string) (table, error) {
	t := table{}

	v, _, err := rec.Map(bin)
	if err != nil {
		return nil, err
	}

	if err := t.offerAll(v); err != nil {
		return nil, err
	}

	return t, nil
}

// The counter's value: everything added minus everything taken.
func valueOf(rec *udf.Record) (int64, error) {
	added, err := stored(rec, "inc")
	if err != nil {
		return 0, err
	}

	taken, err := stored(rec, "dec")
	if err != nil {
		return 0, err
	}

	return added.total() - taken.total(), nil
}

// thisSite is this cluster's site: its XDR src-id, as the decimal string the amounts are keyed by. The
// server knows which cluster runs the write, so no client can count under another cluster's entry.
func thisSite() (string, error) {
	id := udf.ClusterSrcID()
	if id <= 0 {
		return "", errors.New("no XDR src-id on this cluster: set src-id in its xdr stanza")
	}

	return strconv.FormatInt(id, 10), nil
}

// Adds n to this cluster's entry of one map and saves; answers the counter's value.
func grow(rec *udf.Record, bin string, n int64) (int64, error) {
	if n < 0 {
		return 0, errors.New("the amount must not be negative: an entry only grows")
	}

	t, err := stored(rec, bin)
	if err != nil {
		return 0, err
	}

	site, err := thisSite()
	if err != nil {
		return 0, err
	}

	t[site] += n

	if err := rec.Set(bin, t.toMap()); err != nil {
		return 0, err
	}

	if err := rec.Save(); err != nil {
		return 0, err
	}

	return valueOf(rec)
}

// Adds n to this cluster's increments, under its src-id. Returns the counter's value.
//
//udf:function increment
func increment(rec *udf.Record, n int64) (int64, error) {
	return grow(rec, "inc", n)
}

// Adds n to this cluster's decrements, under its src-id. Returns the counter's value.
//
//udf:function decrement
func decrement(rec *udf.Record, n int64) (int64, error) {
	return grow(rec, "dec", n)
}

// The counter's value: the sum of every site's increments minus the sum of every site's decrements.
//
//udf:function value
func value(rec *udf.Record) (int64, error) {
	return valueOf(rec)
}

func main() {}
