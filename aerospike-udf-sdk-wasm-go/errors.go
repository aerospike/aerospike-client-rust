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

// Error is what the typed tier returns when the host refuses a call or a value is not of the kind asked
// for: the native's status and a message naming what failed. A UDF returns it (or any other error) from a
// `//udf:function` or handler, and the generated export fails the call with its message.
type Error struct {
	Status int32
	Msg    string
}

func (e *Error) Error() string { return e.Msg }

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

// FailErr fails the call with the error's message and returns the status an export then returns.
func FailErr(err error) int32 {
	return Fail(err.Error())
}

// Commit writes the staged bins, creating the record if it does not exist.
func Commit() error {
	if Exists() {
		return statusError(recUpdate(), "could not commit the record")
	}
	return statusError(recCreate(), "could not create the record")
}

// Delete deletes the record.
func Delete() error {
	return statusError(recRemove(), "could not delete the record")
}
