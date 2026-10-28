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

// A set with re-adds, in Go: the LWW-Element-Set, merged across data centres by the xdr-merge event. The
// same module as aerospike-udf-sdk-wasm/examples/lwwset.
//
// Two map bins, addedAt and removedAt, hold a timestamp per tag, the server's clock in milliseconds at the
// write. A tag is a member when its add is later than its removal; at equal stamps the removal wins.
// Adding a tag back after a removal is just a later stamp. The merge is the PN-Counter's: for every tag of
// each map, the larger stamp. The merge never reads the clock (the server refuses it inside a handler);
// only the write UDFs stamp, bumping past the tag's last stamp when the clock has not moved.
//
//	event-set:event=xdr-merge;namespace=test;set=tags;module=lwwset;function=merge_lwwset;on-failure=abandon
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	"errors"
	"sort"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

const tagMax = 32

var (
	addedAt   = udf.NewBin("addedAt")
	removedAt = udf.NewBin("removedAt")
)

// The stamps of one map, gathered from one or both sides: for every tag, the later stamp seen.
type stamps map[string]int64

func (s stamps) offer(tag string, at int64) {
	if at > s[tag] {
		s[tag] = at
	}
}

// Every entry of a stored map.
func (s stamps) offerAll(stored udf.Value) error {
	for k, v := range stored.Map() {
		tag, err := k.Str()
		if err != nil {
			return err
		}

		at, err := v.Int()
		if err != nil {
			return err
		}

		s.offer(tag, at)
	}

	return nil
}

func (s stamps) toMap() *udf.Map {
	m := udf.NewMap(max(len(s), 1))

	for tag, at := range s {
		m.Put(tag, at)
	}

	return m
}

// The merge: for every tag of each map, the later stamp. Every other bin is the incoming one's.
//
//udf:on xdr-merge merge_lwwset
func mergeLwwSet(current, incoming *udf.EventRecord, _ *udf.Conflict) (udf.Resolution, error) {
	out := udf.NewMap(2)

	for _, bin := range []string{"addedAt", "removedAt"} {
		s := stamps{}

		for _, side := range []*udf.EventRecord{current, incoming} {
			if stored, ok := side.Get(bin); ok {
				if err := s.offerAll(stored); err != nil {
					return udf.Resolution{}, err
				}
			}
		}

		out.Put(bin, s.toMap())
	}

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}

// Both of the record's maps.
func load() (stamps, stamps, error) {
	a, r := stamps{}, stamps{}

	if v, ok := addedAt.Get(); ok {
		if err := a.offerAll(v); err != nil {
			return nil, nil, err
		}
	}

	if v, ok := removedAt.Get(); ok {
		if err := r.offerAll(v); err != nil {
			return nil, nil, err
		}
	}

	return a, r, nil
}

// Whether a tag is a member: added, and later than removed. A tie goes to the removal.
func isMember(a, r stamps, tag string) bool {
	return a[tag] > 0 && a[tag] > r[tag]
}

// The members, in order, as a built list.
func membersOf() (*udf.List, error) {
	a, r, err := load()
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(a))

	for tag := range a {
		if isMember(a, r, tag) {
			keys = append(keys, tag)
		}
	}

	sort.Strings(keys)

	l := udf.NewList(max(len(keys), 1))

	for _, tag := range keys {
		l.Add(tag)
	}

	return l, nil
}

// Stamps the tag in one of the maps and commits: the clock, or one past the tag's last stamp when the
// clock has not moved since.
func stamp(bin udf.Bin, into stamps, a, r stamps, tag string) error {
	// The Rust module bounds a tag with a Str<32>; the same limit, as a check.
	if len(tag) > tagMax {
		return errors.New("the tag is too long")
	}

	last := max(a[tag], r[tag])
	at := max(udf.Now(), last+1)

	into.offer(tag, at)

	if err := bin.SetMap(into.toMap()); err != nil {
		return err
	}

	return udf.Commit()
}

// Adds a tag, or adds it back. Returns the members.
//
//udf:function add
func add(tag string) (*udf.List, error) {
	a, r, err := load()
	if err != nil {
		return nil, err
	}

	if err := stamp(addedAt, a, a, r, tag); err != nil {
		return nil, err
	}

	return membersOf()
}

// Removes a tag that is a member. Returns the members.
//
//udf:function remove
func remove(tag string) (*udf.List, error) {
	a, r, err := load()
	if err != nil {
		return nil, err
	}

	if !isMember(a, r, tag) {
		return nil, errors.New("not a member")
	}

	if err := stamp(removedAt, r, a, r, tag); err != nil {
		return nil, err
	}

	return membersOf()
}

// The members, in order.
//
//udf:function members
func members() (*udf.List, error) {
	return membersOf()
}

func main() {}
