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

package udf

import "strconv"

// Error is what the typed tier returns when the host refuses a call or a value is not of the kind
// asked for. Status is the status behind it, one of the constants beside OK: NotFound for a bin or an
// argument that is not there, TypeMismatch for a value of another kind, Refused for a write the host
// would not take, Err for anything else. Msg names what failed.
//
// A function under `//udf:function`, or a handler, returns it like any other error, and the generated
// export fails the call with its message. Code that has to tell one failure from another reads Status:
//
//	if err := rec.Save(); err != nil {
//		if e, ok := err.(*udf.Error); ok && e.Status == udf.Refused {
//			udf.Log(udf.LogWarn, "the record is read-only here")
//		}
//		return err
//	}
type Error struct {
	Status int32
	Msg    string
}

// Error is the message, which makes *Error an error.
func (e *Error) Error() string { return e.Msg }

// Needed is the size a buffer has to have, when the error is a buffer that was too small: its Status
// is at or below NeededBase, and this is the -(Status) - 16 behind it. It is 0 for any other error.
//
//	key, ok, err := rec.Key(buf)
//	if e, is := err.(*udf.Error); is && e.Needed() > 0 {
//		buf = make([]byte, e.Needed())
//		key, ok, err = rec.Key(buf)
//	}
func (e *Error) Needed() int {
	if e.Status <= NeededBase {
		return int(-e.Status) - 16
	}
	return 0
}

// StatusOf is the host status an error carries, as a value: OK for nil, the Status of an *Error,
// and Err for an error of any other type, such as one a UDF made itself with errors.New. Every error
// the SDK returns is an *Error, so this is how a caller tells an absent bin (NotFound) from one of
// another kind (TypeMismatch) or a write the host refused (Refused) without reading the message.
//
//	if _, err := rec.IntOr("visits", 0); udf.StatusOf(err) == udf.TypeMismatch {
//		// the bin is there and is not an integer
//	}
func StatusOf(err error) int32 {
	if err == nil {
		return OK
	}
	if e, ok := err.(*Error); ok {
		return e.Status
	}
	return Err
}

// Errorf-free message building, so that fmt stays out of the module.
func errorf(status int32, parts ...string) error {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	b := make([]byte, 0, n)
	for _, p := range parts {
		b = append(b, p...)
	}
	return &Error{Status: status, Msg: string(b)}
}

// statusError turns a native's status into an error, nil for OK.
func statusError(rc int32, what string) error {
	if rc == OK {
		return nil
	}
	return errorf(rc, what, " (", strconv.Itoa(int(rc)), ")")
}

// FailErr fails the call with the error's message, as Fail does, and returns the non-zero status for
// the export to return. err must not be nil. The generated exports call it with what a function
// returned; an export in the raw form calls it with an error from the Arg functions or the record.
func FailErr(err error) int32 {
	return Fail(err.Error())
}
