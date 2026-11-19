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

// Exact top-k over packed float vectors, as a WASM UDF in Go (TinyGo).
//
// The same module as the Rust one (aerospike-udf-sdk-wasm/examples/vector) and the C one
// (aerospike-udf-sdk-wasm-c/examples/vector), step for step - see the Rust module's documentation for the
// shared algorithm. In short: the host copies the blob straight into a float32 array; distances accumulate
// in float64 from float32 elements in one fixed order; larger is always worse; ties break on the digest; the
// arguments are read once per stream. The hot path allocates nothing, so the leaking collector is fine.
//
//	topk("v", <query blob>, "cosine", 10)     stream: the 10 nearest, one partial per stream
//	vec_distance("v", <query blob>, "dot")    record: one number
//
// The stream is the TopK variable under //udf:stream: udfgen (go generate) exports its Record and End
// methods as topk_record and topk_end, and vec_distance under its own directive. All three keep the raw
// (args, argc int32) int32 form, since the arguments are read once per stream and kept; the directives'
// params give the signatures. Record and vec_distance take the *udf.Record first, which the export hands
// them; End takes none, since the stream is over. A stream's record is read-only, and nothing here
// writes.
//
// Build: go generate ./examples/vector && tinygo build -o vector_go.wasm -target=wasm-unknown.json \
// -opt=2 -no-debug -panic=trap -scheduler=none -gc=leaking ./examples/vector
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	"math"
	"strconv"
	"unsafe"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

const (
	maxDim = 2048
	// Bounded by the per-call handle table: each hit of the partial is one built map.
	maxK      = 200
	digestLen = 20
	keyMax    = 64
)

type metric int32

const (
	cosine metric = iota
	dot
	sqEuclidean
	euclidean
)

type hit struct {
	score  float64
	digest [digestLen]byte
	keyLen int
	key    [keyMax]byte
}

// TopK is the stream's state: one instance serves a whole stream.
type TopK struct {
	configured  bool
	bin         string
	metric      metric
	dim         int
	query       [maxDim]float32
	qnorm       float64
	k           int
	n           int
	hits        [maxK]hit
	skippedDim  int64
	skippedNone int64
}

//udf:stream topk params="(bin: str, query: bytes, metric: str, k?: int)" end="()"
var topk TopK

// The record's vector, copied by the host straight into the float array.
var vec [maxDim]float32

var (
	binBuf    [32]byte
	metricBuf [32]byte
	args      [4]udf.Value
)

func parseMetric(name string) (metric, bool) {
	switch name {
	case "", "cosine":
		return cosine, true
	case "dot", "ip", "inner_product":
		return dot, true
	case "sqeuclidean", "l2sq":
		return sqEuclidean, true
	case "euclidean", "l2":
		return euclidean, true
	}
	return cosine, false
}

// Read the arguments once: bin, query, metric, k. Returns "" or the failure message.
func configure(handle, argc int32) string {
	n := udf.Args(handle, argc, args[:])

	// Args reads no more than the window holds, and a call that passed more must not lose them in
	// silence.
	if err := udf.Arity("topk", len(args), argc); err != nil {
		return err.Error()
	}

	if n < 2 || args[0].Kind != udf.KindStr {
		return "topk: bin name missing"
	}

	blen := udf.Bytes(args[0], binBuf[:])
	if blen < 0 {
		return "topk: bin name too long"
	}
	topk.bin = string(binBuf[:blen])

	if args[1].Kind != udf.KindBlob {
		return "topk: the query must be a blob of f32"
	}
	if args[1].Len%4 != 0 || args[1].Len/4 > maxDim {
		return "topk: the query is not a vector of f32 this module can hold"
	}
	topk.dim = int(args[1].Len / 4)

	qbytes := unsafe.Slice((*byte)(unsafe.Pointer(&topk.query[0])), maxDim*4)
	if udf.Bytes(args[1], qbytes) < 0 {
		return "topk: could not read the query"
	}

	qn := 0.0
	for i := 0; i < topk.dim; i++ {
		b := float64(topk.query[i])
		qn += b * b
	}
	topk.qnorm = math.Sqrt(qn)

	name := ""
	if n > 2 && args[2].Kind == udf.KindStr {
		mlen := udf.Bytes(args[2], metricBuf[:])
		if mlen < 0 {
			return "topk: unknown metric"
		}
		name = string(metricBuf[:mlen])
	}

	m, ok := parseMetric(name)
	if !ok {
		return "topk: unknown metric"
	}
	topk.metric = m

	topk.k = 10
	if n > 3 && args[3].Kind == udf.KindInt {
		k := args[3].Payload
		if k < 1 {
			k = 1
		}
		if k > maxK {
			k = maxK
		}
		topk.k = int(k)
	}

	topk.n = 0
	topk.configured = true

	return ""
}

// The measured distance (or similarity, for dot) between the record's vector and the query.
//
// Four accumulators, lane k holding the elements whose index is k mod 4: a single float64 sum cannot be
// vectorised (floating-point addition does not reassociate), four independent ones can live in SIMD
// registers (+simd128 in the target), and they are folded in one fixed order at the end. The same order is in
// the Rust and C modules and in the test's local computation, which keeps the four bit-identical.
func measure() float64 {
	n := topk.dim
	full := n - n%4
	v := vec[:n]
	q := topk.query[:n]

	switch topk.metric {
	case cosine:
		var d, sq [4]float64
		for i := 0; i < full; i += 4 {
			for k := 0; k < 4; k++ {
				a := float64(v[i+k])
				b := float64(q[i+k])
				d[k] += a * b
				sq[k] += a * a
			}
		}
		for k := 0; k < n-full; k++ {
			a := float64(v[full+k])
			b := float64(q[full+k])
			d[k] += a * b
			sq[k] += a * a
		}
		dot := ((d[0] + d[1]) + d[2]) + d[3]
		na := ((sq[0] + sq[1]) + sq[2]) + sq[3]
		if na == 0 || topk.qnorm == 0 {
			return 1.0
		}
		return 1.0 - dot/(math.Sqrt(na)*topk.qnorm)
	case dot:
		var d [4]float64
		for i := 0; i < full; i += 4 {
			for k := 0; k < 4; k++ {
				d[k] += float64(v[i+k]) * float64(q[i+k])
			}
		}
		for k := 0; k < n-full; k++ {
			d[k] += float64(v[full+k]) * float64(q[full+k])
		}
		return ((d[0] + d[1]) + d[2]) + d[3]
	default:
		var sq [4]float64
		for i := 0; i < full; i += 4 {
			for k := 0; k < 4; k++ {
				e := float64(v[i+k]) - float64(q[i+k])
				sq[k] += e * e
			}
		}
		for k := 0; k < n-full; k++ {
			e := float64(v[full+k]) - float64(q[full+k])
			sq[k] += e * e
		}
		sum := ((sq[0] + sq[1]) + sq[2]) + sq[3]
		if topk.metric == euclidean {
			return math.Sqrt(sum)
		}
		return sum
	}
}

// Larger is worse.
func scoreOf(measured float64) float64 {
	if topk.metric == dot {
		return -measured
	}
	return measured
}

// The record's vector into vec: 1 read, 0 skipped and counted, -1 a failure.
func readVector(rec *udf.Record) int {
	// Get, not Bin: a record without the vector is ordinary here, and Get says so without making an
	// error for each one.
	v, ok, err := rec.Get(topk.bin)
	if err != nil {
		return -1
	}

	if !ok || v.Kind != udf.KindBlob {
		topk.skippedNone++
		return 0
	}
	if int(v.Len) != topk.dim*4 {
		topk.skippedDim++
		return 0
	}

	bytes := unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), maxDim*4)
	if udf.Bytes(v, bytes) < 0 {
		return -1
	}
	return 1
}

func digestLess(a, b *[digestLen]byte) int {
	for i := 0; i < digestLen; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// (score, digest) < (score, digest)
func better(a, b *hit) bool {
	if a.score != b.score {
		return a.score < b.score
	}
	return digestLess(&a.digest, &b.digest) < 0
}

// Offer a candidate: kept if it beats the worst of the k, ties broken on the digest.
func offer(rec *udf.Record, score float64) bool {
	if topk.n == topk.k && score > topk.hits[topk.n-1].score {
		return true
	}

	var h hit
	h.score = score

	if d, err := rec.Digest(h.digest[:]); err != nil || len(d) != digestLen {
		return false
	}

	if topk.n == topk.k && score == topk.hits[topk.n-1].score && digestLess(&h.digest, &topk.hits[topk.n-1].digest) >= 0 {
		return true
	}

	// A string or blob key is copied by the host straight into the hit; an integer one is written out.
	// A hit carries a key of keyMax bytes at most: a longer one is an error here, and the hit goes
	// without it, since the digest is what identifies the record.
	if key, ok, err := rec.Key(h.key[:]); err == nil && ok {
		if key.Kind == udf.KindInt {
			h.keyLen = copy(h.key[:], strconv.FormatInt(key.Payload, 10))
		} else {
			h.keyLen = int(key.Len)
		}
	}

	at := topk.n
	for at > 0 && better(&h, &topk.hits[at-1]) {
		at--
	}

	last := topk.n
	if topk.n == topk.k {
		last = topk.k - 1
	}
	if at > last {
		return true
	}

	for i := last; i > at; i-- {
		topk.hits[i] = topk.hits[i-1]
	}
	topk.hits[at] = h

	if topk.n < topk.k {
		topk.n++
	}
	return true
}

// Record is the per-record half, in the raw form, with the record first.
func (t *TopK) Record(rec *udf.Record, handle, argc int32) int32 {
	if !topk.configured {
		if msg := configure(handle, argc); msg != "" {
			return udf.Fail(msg)
		}
	}

	switch readVector(rec) {
	case -1:
		return udf.Fail("topk: could not read the vector")
	case 0:
		return 0
	}

	if !offer(rec, scoreOf(measure())) {
		return udf.Fail("topk: could not read the digest")
	}
	return 0
}

var (
	keyScore  = "score"
	keyDigest = "digest"
	keyKey    = "key"
)

// End is the closing half: the partial, k hits at most. It runs once per stream, so it is written on the
// builders; each hit still goes to the host as one batch of three entries.
func (t *TopK) End(handle, argc int32) int32 {
	out := udf.NewMap(3)
	hits := udf.NewList(topk.n)

	if out.Err() != nil || hits.Err() != nil {
		return udf.Fail("topk: could not build the partial")
	}

	for i := 0; i < topk.n; i++ {
		h := &topk.hits[i]
		pairs := [6]udf.Value{
			udf.StringValue(keyScore), udf.DoubleValue(h.score),
			udf.StringValue(keyDigest), udf.BlobValue(h.digest[:]),
			udf.StringValue(keyKey), udf.StringBytesValue(h.key[:h.keyLen]),
		}

		hit := udf.NewMap(3).PutEntries(pairs[:])
		hits.Add(hit)
		hit.Close() // hits keeps it: one handle per hit, given back as it goes
		if hits.Err() != nil {
			return udf.Fail("topk: could not build a hit")
		}
	}

	out.Put("hits", hits).Put("skipped_dim", topk.skippedDim).Put("skipped_none", topk.skippedNone)

	if udf.ResultMap(out) != nil {
		return udf.Fail("topk: could not return the partial")
	}
	return 0
}

// The distance from this record's vector to the query: one record, one number, nil for no such vector.
//
//udf:function vec_distance params="(bin: str, query: bytes, metric: str)"
func vecDistance(rec *udf.Record, handle, argc int32) int32 {
	if msg := configure(handle, argc); msg != "" {
		return udf.Fail(msg)
	}

	switch readVector(rec) {
	case -1:
		return udf.Fail("vec_distance: could not read the vector")
	case 0:
		udf.ResultNil()
	default:
		udf.ResultDouble(measure())
	}
	return 0
}

func main() {}
