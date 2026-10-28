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

// Guards in Go: the same handlers as aerospike-udf-sdk-wasm/examples/guard - a before-write that refuses a
// negative balance and stamps every write that gets through, a before-delete lock, a truncate fence, and
// a timer that counts ticks and re-arms itself up to three.
//
// The directives are the exports: udfgen (go generate) writes udf_exports.go with the //export
// trampolines that decode each event into the handler's parameters and deliver its answer, and the
// signature exports event-set checks.
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import (
	"strings"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

var ticks = udf.NewBin("ticks")

// Refuses a negative balance; otherwise stamps the write's source and counts it, keeping every other bin.
//
//udf:on before-write guard_account
func guardAccount(before, after *udf.EventRecord, w udf.WriteInfo) (udf.Decision, error) {
	if balance, ok := after.Int("balance"); ok && balance < 0 {
		return udf.Reject(22, "balance may not go negative"), nil
	}

	writes, _ := before.Int("writes")
	out := udf.NewMap(4).
		Put("stamped", w.Source).
		Put("writes", writes+1)

	return udf.AcceptRecord(after.WithBins(out)), out.Err()
}

//udf:on before-delete guard_delete
func guardDelete(rec *udf.EventRecord, _ udf.DeleteInfo) (udf.Gate, error) {
	if locked, ok := rec.Bool("locked"); ok && locked {
		return udf.Refuse(22, "record is locked"), nil
	}

	return udf.Allow(), nil
}

//udf:on before-truncate guard_truncate
func guardTruncate(t udf.TruncateInfo) (udf.Gate, error) {
	if strings.HasPrefix(t.Set, "keep") {
		return udf.Refuse(22, "this set is protected from truncation"), nil
	}

	return udf.Allow(), nil
}

// The timer handler runs as a record UDF: the record functions work.
//
//udf:on timer tick
func tick(_ udf.TimerInfo) error {
	n := ticks.IntOr(0) + 1

	if err := ticks.SetInt(n); err != nil {
		return err
	}

	if err := udf.Commit(); err != nil {
		return err
	}

	if n < 3 {
		udf.TimerSet("tick", 200)
	}

	return nil
}

func main() {}
