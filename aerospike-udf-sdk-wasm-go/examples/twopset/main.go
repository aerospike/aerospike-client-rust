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

// A set with removals, in Go: the 2P-Set, merged across data centres by the xdr-merge event. The same
// module as aerospike-udf-sdk-wasm/examples/twopset.
//
// Two list bins, added and removed, both only ever grow; a tag is a member when it is in added and not in
// removed; the merge takes the union of each. A tag in removed is a tombstone: it beats a concurrent add
// on the other side, and it stays forever, which is why a removed tag can never come back. Both lists are
// stored sorted and without duplicates, so the same set is always the same bytes.
//
//	event-set:event=xdr-merge;namespace=test;set=tags;module=twopset;function=merge_twopset;on-failure=abandon
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	"errors"
	"sort"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

const tagMax = 32

var (
	added   = udf.NewBin("added")
	removed = udf.NewBin("removed")
)

// A set of tags, gathered from one or both sides.
type tags map[string]bool

// Every item of a stored list.
func (t tags) offerAll(stored udf.Value) error {
	for item := range stored.List() {
		tag, err := item.Str()
		if err != nil {
			return err
		}

		t[tag] = true
	}

	return nil
}

// The tags not in except, as a built list, in order.
func (t tags) toList(except tags) *udf.List {
	keys := make([]string, 0, len(t))

	for tag := range t {
		if !except[tag] {
			keys = append(keys, tag)
		}
	}

	sort.Strings(keys)

	l := udf.NewList(max(len(keys), 1))

	for _, tag := range keys {
		l.Add(tag)
	}

	return l
}

// The merge: the union of each list. Every other bin is the incoming one's.
//
//udf:on xdr-merge merge_twopset
func mergeTwoPSet(current, incoming *udf.EventRecord, _ *udf.Conflict) (udf.Resolution, error) {
	out := udf.NewMap(2)

	for _, bin := range []string{"added", "removed"} {
		t := tags{}

		for _, side := range []*udf.EventRecord{current, incoming} {
			if stored, ok := side.Get(bin); ok {
				if err := t.offerAll(stored); err != nil {
					return udf.Resolution{}, err
				}
			}
		}

		out.Put(bin, t.toList(nil))
	}

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}

// One of the record's lists.
func stored(bin udf.Bin) (tags, error) {
	t := tags{}

	if v, ok := bin.Get(); ok {
		if err := t.offerAll(v); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// The members: added and not removed.
func membersOf() (*udf.List, error) {
	a, err := stored(added)
	if err != nil {
		return nil, err
	}

	r, err := stored(removed)
	if err != nil {
		return nil, err
	}

	return a.toList(r), nil
}

// Adds a tag to one of the lists and commits.
func grow(bin udf.Bin, tag string) error {
	// The Rust module bounds a tag with a Str<32>; the same limit, as a check.
	if len(tag) > tagMax {
		return errors.New("the tag is too long")
	}

	t, err := stored(bin)
	if err != nil {
		return err
	}

	if t[tag] {
		return nil
	}

	t[tag] = true

	if err := bin.SetList(t.toList(nil)); err != nil {
		return err
	}

	return udf.Commit()
}

// Adds a tag. Returns the members. A tag removed before stays removed.
//
//udf:function add
func add(tag string) (*udf.List, error) {
	if err := grow(added, tag); err != nil {
		return nil, err
	}

	return membersOf()
}

// Removes a tag that is a member, forever. Returns the members.
//
//udf:function remove
func remove(tag string) (*udf.List, error) {
	a, err := stored(added)
	if err != nil {
		return nil, err
	}

	r, err := stored(removed)
	if err != nil {
		return nil, err
	}

	if !a[tag] || r[tag] {
		return nil, errors.New("not a member")
	}

	if err := grow(removed, tag); err != nil {
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
