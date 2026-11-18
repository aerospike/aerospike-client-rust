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

// The Go side of the book's tutorials. The pages record-udfs/tutorial.md and events/tutorial.md include
// the code between the ANCHOR comments, so what the book shows is what compiles here.
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

// ANCHOR: setup
import (
	"errors"

	udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"
)

// ANCHOR_END: setup

// ANCHOR: visit
// Counts a visit and answers the new count.
//
//udf:function visit
func visit(rec *udf.Record, by int64) (int64, error) {
	visits, _ := rec.Int("visits") // 0 when the bin is absent
	n := visits + by

	if err := rec.Set("visits", n); err != nil { // staged, not yet written
		return 0, err
	}

	return n, rec.Save() // creates the record when there is none
}

// ANCHOR_END: visit

// ANCHOR: withdraw
// Takes amount off the balance, or fails when there is not enough.
//
//udf:function withdraw
func withdraw(rec *udf.Record, amount int64) (int64, error) {
	balance, err := rec.Bin("balance") // an error when the bin is absent
	if err != nil {
		return 0, err
	}

	have, err := balance.Int() // an error when it is not an integer
	if err != nil {
		return 0, err
	}

	if amount > have {
		return 0, errors.New("insufficient funds")
	}

	if err := rec.Set("balance", have-amount); err != nil {
		return 0, err
	}

	return have - amount, rec.Save()
}

// ANCHOR_END: withdraw

// ANCHOR: rename
// Changes the owner; false when the name is already that.
//
//udf:function rename
func rename(rec *udf.Record, name string) (bool, error) {
	owner, err := rec.Bin("owner")
	if err != nil {
		return false, err
	}

	current, err := owner.Str() // copied out of the record
	if err != nil {
		return false, err
	}

	if current == name {
		return false, nil
	}

	if err := rec.Set("owner", name); err != nil {
		return false, err
	}

	return true, rec.Save()
}

// ANCHOR_END: rename

// ANCHOR: total
// The sum of the payments in history.
//
//udf:function total
func total(rec *udf.Record) (int64, error) {
	var sum int64

	for payment := range rec.List("history") {
		n, err := payment.Int()
		if err != nil {
			return 0, err
		}
		sum += n
	}

	return sum, nil
}

// ANCHOR_END: total

// ANCHOR: limit_for
// The limit of one kind, 0 when there is none.
//
//udf:function limit_for
func limitFor(rec *udf.Record, kind string) (int64, error) {
	limits, err := rec.Bin("limits")
	if err != nil {
		return 0, err
	}

	limit, ok := limits.Lookup(kind)
	if !ok { // no such key
		return 0, nil
	}

	return limit.Int()
}

// ANCHOR_END: limit_for

// ANCHOR: limits_total
// The sum of every limit.
//
//udf:function limits_total
func limitsTotal(rec *udf.Record) (int64, error) {
	var sum int64

	for _, limit := range rec.Map("limits") {
		n, err := limit.Int()
		if err != nil {
			return 0, err
		}
		sum += n
	}

	return sum, nil
}

// ANCHOR_END: limits_total

// ANCHOR: log_payment
// Appends a payment to history and answers the new length.
//
//udf:function log_payment
func logPayment(rec *udf.Record, amount int64) (int64, error) {
	stored, err := rec.Bin("history")
	if err != nil {
		return 0, err
	}

	count := int(stored.Len) + 1
	out := udf.NewList(count).AddAll(stored).Add(amount) // the stored elements, then the new one

	if err := rec.Set("history", out); err != nil {
		return 0, err
	}

	return int64(count), rec.Save()
}

// ANCHOR_END: log_payment

// ANCHOR: summary
// The account at a glance, as a map.
//
//udf:function summary
func summary(rec *udf.Record) (*udf.Map, error) {
	name, _ := rec.Get("owner")
	amount, _ := rec.Get("balance")

	out := udf.NewMap(5).
		Put("owner", name). // a stored value goes in as it is
		Put("balance", amount).
		Put("generation", rec.Generation()).
		Put("bins", rec.BinCount()).
		Put("ttl", rec.TTL())

	return out, out.Err()
}

// ANCHOR_END: summary

// ANCHOR: renew
// Counts a renewal and gives the record seconds more to live.
//
//udf:function renew
func renew(rec *udf.Record, seconds int64) (int64, error) {
	renewals, _ := rec.Int("renewals")
	n := renewals + 1

	if err := rec.Set("renewals", n); err != nil {
		return 0, err
	}

	if err := rec.SetTTL(int32(seconds)); err != nil { // takes effect with the save
		return 0, err
	}

	return n, rec.Save()
}

// ANCHOR_END: renew

// ANCHOR: clear_note
// Drops the note bin.
//
//udf:function clear_note
func clearNote(rec *udf.Record) error {
	if err := rec.Remove("note"); err != nil { // staged, like a write
		return err
	}

	return rec.Save()
}

// ANCHOR_END: clear_note

// ANCHOR: close
// Deletes the record.
//
//udf:function close
func closeAccount(rec *udf.Record) error {
	return rec.Delete() // no save: the delete is the write
}

// ANCHOR_END: close

// ANCHOR: no_overdraft
// Refuses a write that leaves a negative balance.
//
//udf:on before-write no_overdraft
func noOverdraft(_, after *udf.EventRecord, _ udf.WriteInfo) (udf.Decision, error) {
	if amount, ok := after.Int("balance"); ok && amount < 0 { // ok is false when absent or nil
		return udf.Reject(22, "balance may not go negative"), nil
	}

	return udf.Accept(), nil
}

// ANCHOR_END: no_overdraft

// ANCHOR: stamp
// Counts the write and records where it came from.
//
//udf:on before-write stamp
func stamp(before, after *udf.EventRecord, w udf.WriteInfo) (udf.Decision, error) {
	writes, _ := before.Int("writes")

	out := udf.NewMap(2).
		Put("writes", writes+1).
		Put("source", w.Source)

	// Every bin the write produced, with the two above on top.
	return udf.AcceptRecord(after.WithBins(out)), out.Err()
}

// ANCHOR_END: stamp

// ANCHOR: keep_locked
// Refuses to delete a locked record.
//
//udf:on before-delete keep_locked
func keepLocked(rec *udf.EventRecord, _ udf.DeleteInfo) (udf.Gate, error) {
	if locked, _ := rec.Bool("locked"); locked {
		return udf.Refuse(22, "record is locked"), nil
	}

	return udf.Allow(), nil
}

// ANCHOR_END: keep_locked

// ANCHOR: hide_secret
// Reads never see the secret bin.
//
//udf:on after-read hide_secret
func hideSecret(rec *udf.EventRecord, _ udf.ReadInfo) (udf.ReadResult, error) {
	if !rec.Has("secret") {
		return udf.Unchanged(), nil
	}

	out := udf.NewMap(1).Put("secret", nil) // nil in an answer: no such bin

	return udf.ReadRecord(rec.WithBins(out)), out.Err()
}

// ANCHOR_END: hide_secret

// ANCHOR: keep_marked
// A record marked keep gets another hour.
//
//udf:on before-expire keep_marked
func keepMarked(rec *udf.EventRecord, _ udf.ExpireInfo) (udf.Expiry, error) {
	if keep, _ := rec.Bool("keep"); keep {
		return udf.Keep(3600), nil
	}

	return udf.Expire(), nil
}

// ANCHOR_END: keep_marked

// ANCHOR: tick
// Counts a tick and re-arms itself, up to three.
//
//udf:on timer tick
func tick(rec *udf.Record, _ udf.TimerInfo) error {
	ticks, _ := rec.Int("ticks") // a timer runs as a record UDF: it gets the record, and may write it
	n := ticks + 1

	if err := rec.Set("ticks", n); err != nil {
		return err
	}

	if err := rec.Save(); err != nil {
		return err
	}

	if n < 3 {
		udf.TimerSet("tick", 200) // run again in 200 ms
	}

	return nil
}

// ANCHOR_END: tick

func main() {}
