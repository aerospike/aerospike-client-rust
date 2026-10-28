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

// An XDR merge, the same as the Rust, C and Java resolvers: visits take the larger count, tags unite in
// LUT order, the profile is whichever side wrote it last, a bin deleted by the later writer stays deleted,
// and everything else is the newer side's. See aerospike-udf-sdk-wasm/examples/resolver for why each rule
// is the way it is; this is that module line for line, over udf.Pair and udf.Both.
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

//udf:on xdr-merge merge_account
func mergeAccount(local, remote *udf.EventRecord, conflict *udf.Conflict) (udf.Resolution, error) {
	pair := conflict.Pair()
	out := udf.NewMap(8)

	// A count each DC advanced on its own: both sides' increments are real, so the merge is the larger
	// of the two, not the sum (the sum would double-count the visits both already knew of).
	if visits, ok := udf.Max(pair.Int("visits")); ok {
		out.Put("visits", visits)
	}

	// A set each DC added to: the union. Its order is part of the result, so it is the order both DCs
	// agree on - the older side's tags, then the newer side's new ones - never "mine first".
	older, hasOlder, newer, hasNewer := pair.List("tags").Ordered()
	tags := udf.NewList(16)

	if hasOlder {
		tags.AddAll(older)
	}

	if hasNewer {
		for tag := range newer.List() {
			text, err := tag.Str()

			if err != nil {
				return udf.Resolution{}, err
			}

			if !hasOlder || !udf.Contains(older, text) {
				tags.Add(tag)
			}
		}
	}

	out.Put("tags", tags)

	// A document one side rewrote whole: last writer wins, by LUT, which both DCs agree on.
	if profile, ok := pair.Bin("profile").Newer(); ok {
		out.Put("profile", profile)
	}

	// A bin one side deleted stays deleted when the delete is the later write for that bin: a nil in the
	// returned record is "no such bin". Without this, the other side's copy would come back through
	// WithNewer below - the resurrection every merge has to guard against.
	applyDeletes(pair, out, local, false)
	applyDeletes(pair, out, remote, true)

	// Everything else is the newer side's - the policy's answer for a bin this handler has no opinion on.
	// `local.WithBins(out)` would read more naturally and be wrong: it prefers a side, and the other DC
	// prefers the other side.
	return udf.Resolve(pair.WithNewer(out)), out.Err()
}

func applyDeletes(pair udf.Pair, out *udf.Map, side *udf.EventRecord, byRemote bool) {
	if !side.DeletedAny() {
		return
	}

	for name := range side.Deleted.List() {
		if bin, err := name.Str(); err == nil && pair.RemoteNewer(bin) == byRemote {
			out.Put(bin, nil)
		}
	}
}

func main() {}
