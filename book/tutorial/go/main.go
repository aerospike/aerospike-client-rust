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

var (
	visits   = udf.NewBin("visits")
	balance  = udf.NewBin("balance")
	owner    = udf.NewBin("owner")
	history  = udf.NewBin("history")
	limits   = udf.NewBin("limits")
	renewals = udf.NewBin("renewals")
	note     = udf.NewBin("note")
	ticks    = udf.NewBin("ticks")
)

// ANCHOR_END: setup

// ANCHOR: visit
// Counts a visit and answers the new count.
//
//udf:function visit
func visit(by int64) (int64, error) {
	n := visits.IntOr(0) + by // 0 when the bin is absent

	if err := visits.SetInt(n); err != nil { // staged, not yet written
		return 0, err
	}

	return n, udf.Commit() // creates the record when there is none
}

// ANCHOR_END: visit

// ANCHOR: withdraw
// Takes amount off the balance, or fails when there is not enough.
//
//udf:function withdraw
func withdraw(amount int64) (int64, error) {
	have, err := balance.Int() // an error when absent or not an integer
	if err != nil {
		return 0, err
	}

	if amount > have {
		return 0, errors.New("insufficient funds")
	}

	if err := balance.SetInt(have - amount); err != nil {
		return 0, err
	}

	return have - amount, udf.Commit()
}

// ANCHOR_END: withdraw

// ANCHOR: rename
// Changes the owner; false when the name is already that.
//
//udf:function rename
func rename(name string) (bool, error) {
	current, err := owner.Str() // copied out of the record
	if err != nil {
		return false, err
	}

	if current == name {
		return false, nil
	}

	if err := owner.SetStr(name); err != nil {
		return false, err
	}

	return true, udf.Commit()
}

// ANCHOR_END: rename

// ANCHOR: total
// The sum of the payments in history.
//
//udf:function total
func total() (int64, error) {
	var sum int64

	for payment := range history.List() {
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
func limitFor(kind string) (int64, error) {
	m, ok := limits.Get()
	if !ok {
		return 0, errors.New("bin limits is absent")
	}

	limit, ok := m.Lookup(kind)
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
func limitsTotal() (int64, error) {
	var sum int64

	for _, limit := range limits.Map() {
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
func logPayment(amount int64) (int64, error) {
	stored, ok := history.Get()
	if !ok {
		return 0, errors.New("bin history is absent")
	}

	count := int(stored.Len) + 1
	out := udf.NewList(count).AddAll(stored).Add(amount) // the stored elements, then the new one

	if err := history.SetList(out); err != nil {
		return 0, err
	}

	return int64(count), udf.Commit()
}

// ANCHOR_END: log_payment

// ANCHOR: summary
// The account at a glance, as a map.
//
//udf:function summary
func summary() (*udf.Map, error) {
	name, _ := owner.Get()
	amount, _ := balance.Get()

	out := udf.NewMap(5).
		Put("owner", name). // a stored value goes in as it is
		Put("balance", amount).
		Put("generation", udf.Generation()).
		Put("bins", udf.BinCount()).
		Put("ttl", udf.TTL())

	return out, out.Err()
}

// ANCHOR_END: summary

// ANCHOR: renew
// Counts a renewal and gives the record seconds more to live.
//
//udf:function renew
func renew(seconds int64) (int64, error) {
	n := renewals.IntOr(0) + 1

	if err := renewals.SetInt(n); err != nil {
		return 0, err
	}

	if err := udf.SetTTL(int32(seconds)); err != nil { // takes effect with the commit
		return 0, err
	}

	return n, udf.Commit()
}

// ANCHOR_END: renew

// ANCHOR: clear_note
// Drops the note bin.
//
//udf:function clear_note
func clearNote() error {
	if err := note.SetNull(); err != nil { // a nil write drops the bin
		return err
	}

	return udf.Commit()
}

// ANCHOR_END: clear_note

// ANCHOR: close
// Deletes the record.
//
//udf:function close
func closeAccount() error {
	return udf.Delete() // no commit: the delete is the write
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
func tick(_ udf.TimerInfo) error {
	n := ticks.IntOr(0) + 1 // the record API works here

	if err := ticks.SetInt(n); err != nil {
		return err
	}

	if err := udf.Commit(); err != nil {
		return err
	}

	if n < 3 {
		udf.TimerSet("tick", 200) // run again in 200 ms
	}

	return nil
}

// ANCHOR_END: tick

func main() {}
