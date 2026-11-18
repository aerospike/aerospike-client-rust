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

//==========================================================
// What the generated exports call: an argument decoded into a parameter of the function's type, failing
// the call with a message that names the argument when it is missing or of another kind, the way the
// Rust SDK's export! rejects a call. The arguments come from Args as a Value slice and a count.
//

// Arity fails a call that passed more arguments than the function declares.
func Arity(fn string, declared int, got int32) error {
	if int(got) > declared {
		return errorf(Err, fn, " takes at most ", strconv.Itoa(declared), " arguments, got ", strconv.Itoa(int(got)))
	}
	return nil
}

func argAt(a []Value, n int32, i int, fn, p string) (Value, error) {
	if i >= int(n) || a[i].Kind == KindAbsent {
		return Value{Kind: KindAbsent}, errorf(NotFound, fn, ": argument ", strconv.Itoa(i+1), " (", p, ") is missing")
	}
	return a[i], nil
}

func argNone(a []Value, n int32, i int) bool {
	return i >= int(n) || a[i].Kind == KindAbsent || a[i].Kind == KindNil
}

func mismatch(fn string, i int, p, want string) error {
	return errorf(TypeMismatch, fn, ": argument ", strconv.Itoa(i+1), " (", p, ") is not ", want)
}

// ArgInt is argument i as an integer. The error, which names the function fn and the parameter p, says
// the argument is missing or is not an int.
func ArgInt(a []Value, n int32, i int, fn, p string) (int64, error) {
	v, err := argAt(a, n, i, fn, p)
	if err != nil {
		return 0, err
	}
	if v.Kind != KindInt {
		return 0, mismatch(fn, i, p, "an int")
	}
	return v.Payload, nil
}

// ArgInt32 is argument i as a 32-bit integer; an error when it is missing, not an int, or does not fit 32
// bits.
func ArgInt32(a []Value, n int32, i int, fn, p string) (int32, error) {
	x, err := ArgInt(a, n, i, fn, p)
	if err != nil {
		return 0, err
	}
	if int64(int32(x)) != x {
		return 0, mismatch(fn, i, p, "a 32-bit int")
	}
	return int32(x), nil
}

// ArgDouble is a double, or an int widened to one.
func ArgDouble(a []Value, n int32, i int, fn, p string) (float64, error) {
	v, err := argAt(a, n, i, fn, p)
	if err != nil {
		return 0, err
	}
	switch v.Kind {
	case KindDouble:
		return v.double(), nil
	case KindInt:
		return float64(v.Payload), nil
	}
	return 0, mismatch(fn, i, p, "a double")
}

// ArgBool is argument i as a boolean; an error when it is missing or not a bool.
func ArgBool(a []Value, n int32, i int, fn, p string) (bool, error) {
	v, err := argAt(a, n, i, fn, p)
	if err != nil {
		return false, err
	}
	if v.Kind != KindBool {
		return false, mismatch(fn, i, p, "a bool")
	}
	return v.Payload != 0, nil
}

// ArgString is argument i as a string, copied out; an error when it is missing or not a string.
func ArgString(a []Value, n int32, i int, fn, p string) (string, error) {
	v, err := argAt(a, n, i, fn, p)
	if err != nil {
		return "", err
	}
	if v.Kind != KindStr {
		return "", mismatch(fn, i, p, "a string")
	}
	return v.String()
}

// ArgBlob is the bytes of a string or a blob.
func ArgBlob(a []Value, n int32, i int, fn, p string) ([]byte, error) {
	v, err := argAt(a, n, i, fn, p)
	if err != nil {
		return nil, err
	}
	if v.Kind != KindStr && v.Kind != KindBlob {
		return nil, mismatch(fn, i, p, "a string or a blob")
	}
	return v.Blob()
}

// ArgValue is any value, nil included.
func ArgValue(a []Value, n int32, i int, fn, p string) (Value, error) {
	return argAt(a, n, i, fn, p)
}

// ArgRest is every argument from i on.
func ArgRest(a []Value, n int32, i int) []Value {
	if i >= int(n) {
		return nil
	}
	out := make([]Value, int(n)-i)
	copy(out, a[i:n])
	return out
}

// The optional forms: nil when the argument is absent or nil.

// OptInt is argument i as an integer, nil when it is absent or nil; an error when it is of another kind.
func OptInt(a []Value, n int32, i int, fn, p string) (*int64, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	x, err := ArgInt(a, n, i, fn, p)
	return &x, err
}

// OptDouble is argument i as a double, or an int widened to one; nil when it is absent or nil, an error
// when it is of another kind.
func OptDouble(a []Value, n int32, i int, fn, p string) (*float64, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	x, err := ArgDouble(a, n, i, fn, p)
	return &x, err
}

// OptBool is argument i as a boolean, nil when it is absent or nil; an error when it is of another kind.
func OptBool(a []Value, n int32, i int, fn, p string) (*bool, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	x, err := ArgBool(a, n, i, fn, p)
	return &x, err
}

// OptString is argument i as a string, nil when it is absent or nil; an error when it is of another kind.
func OptString(a []Value, n int32, i int, fn, p string) (*string, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	x, err := ArgString(a, n, i, fn, p)
	return &x, err
}

// OptValue is argument i as a value of any kind, nil when it is absent or nil. The pointer is into a, and
// the error is always nil.
func OptValue(a []Value, n int32, i int, fn, p string) (*Value, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	return &a[i], nil
}
