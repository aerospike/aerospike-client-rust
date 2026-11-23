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

// A record of registers, in Go: every bin is a last-writer-wins register, and the stamps are the
// server's. The same module as aerospike-udf-sdk/wasm-rust/examples/registers.
//
// examples/lwwset keeps a timestamp per element inside the record, and its own UDFs write them. A
// namespace with bin convergence (conflict-resolve-writes true) already keeps one for every bin: when the
// bin was last written, and which cluster wrote it. A side of the xdr-merge event gives both,
// BinLastUpdateTime(name) and BinSourceID(name), and the pair is a register's stamp: the later write wins,
// and of two writes in the same millisecond, the one from the cluster with the larger id.
//
// A register therefore has no write UDF. A client puts theme, lang or email as it would any bin, and
// deletes one by writing nil to it; the server stamps the bin, and merge_settings takes the later side,
// one bin at a time. A deleted bin is still listed, by DeletedBins(), and keeps its stamp: a delete that
// is the later write stays a delete, and one that is the earlier write does not undo what followed it.
//
// The server does all of that by itself under bin convergence. The handler exists for the one bin that is
// not a register: seen, the highest message number this user has read on any device. Its merge is the
// larger number, whichever side wrote last, which no policy that picks a side can express. It has the
// module's only write UDF, saw(n), which never lowers it: a merge can keep a maximum only when the writes
// kept one too.
//
//	event-set:event=xdr-merge;namespace=testbc;set=settings;module=registers;function=merge_settings;on-failure=abandon
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go/cmd/udfgen

import (
	"errors"
	"iter"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go"
)

// A register's stamp on one side: when the bin was last written there, and by which cluster. The zero
// stamp is a side with nothing to say about the bin, and it is less than any other.
type stamp struct {
	known    bool
	at       int64
	sourceID int64
}

// Whether s is the strictly later stamp: by time first, then by source id.
func (s stamp) after(o stamp) bool {
	if s.known != o.known {
		return s.known
	}

	if s.at != o.at {
		return s.at > o.at
	}

	return s.sourceID > o.sourceID
}

// The side's stamp for a bin it has or lists as deleted; the zero stamp otherwise.
func stampOf(side *udf.EventRecord, name string) stamp {
	if side.Has(name) || side.Deleted(name) {
		return stamp{known: true, at: side.BinLastUpdateTime(name), sourceID: side.BinSourceID(name)}
	}

	return stamp{}
}

// The merge: seen is the larger number; every other bin is the side that wrote it last.
//
//udf:on xdr-merge merge_settings
func mergeSettings(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	// Without bin convergence there is no stamp per bin, and a merge that pretended otherwise would
	// decide every bin by the record's time. Failing hands the write to the binding's on-failure.
	if !current.UsesBinConvergence() {
		return udf.Resolution{}, errors.New("registers need a namespace with conflict-resolve-writes")
	}

	out := udf.NewMap(16)

	a, hasA, err := current.Int("seen")
	if err != nil {
		return udf.Resolution{}, err
	}

	b, hasB, err := incoming.Int("seen")
	if err != nil {
		return udf.Resolution{}, err
	}

	if hasA && hasB {
		out.Put("seen", max(a, b))
	} else if hasA {
		out.Put("seen", a)
	} else if hasB {
		out.Put("seen", b)
	}

	for _, side := range []*udf.EventRecord{current, incoming} {
		for _, names := range []iter.Seq[string]{side.BinNames(), side.DeletedBins()} {
			for name := range names {
				// The first pass decided every bin the current side knows of.
				if name == "seen" || (side == incoming && stampOf(current, name).known) {
					continue
				}

				winner := current

				if stampOf(incoming, name).after(stampOf(current, name)) {
					winner = incoming
				}

				// A nil in the answer is "no such bin": the winner's delete, kept.
				v, ok, err := winner.Get(name)
				if err != nil {
					return udf.Resolution{}, err
				}

				if ok {
					out.Put(name, v)
				} else {
					out.Put(name, nil)
				}
			}
		}
	}

	return udf.Resolve(out), out.Err()
}

// Records that message n was read. Returns the highest number read so far.
//
//udf:function saw
func saw(rec *udf.Record, n int64) (int64, error) {
	seen, err := rec.IntOr("seen", 0)
	if err != nil {
		return 0, err
	}

	if n > seen {
		if err := rec.Set("seen", n); err != nil {
			return 0, err
		}

		if err := rec.Save(); err != nil {
			return 0, err
		}
	}

	return max(seen, n), nil
}

func main() {}
