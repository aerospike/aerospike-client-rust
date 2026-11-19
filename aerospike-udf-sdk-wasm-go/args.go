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

// Arity fails a call that passed more arguments than the function declares: the error reads
// "<fn> takes at most <declared> arguments, got <got>", naming the limit and the count. fn is the
// UDF's name, for the message, and got is the argc the export was called with. Fewer arguments are
// not its business: a missing one is reported by the Arg function that reads it.
//
// Every generated export calls it. For a function whose last parameter takes the rest, declared is
// MaxArgs, the most a UDF takes in total, so a call with 129 arguments fails and none is dropped.
// An export in the raw form calls it with the length of the window it handed Args.
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

// ArgInt is argument i as an integer.
//
// The Arg functions all take the same five parameters. a and n are the slice Args filled and the count
// it returned. i is the argument's index, from 0. fn is the UDF's name and p the parameter's, and they
// are used only in the error, which reads "<fn>: argument <i+1> (<p>) is missing" or "... is not an
// int": the message counts arguments from 1, and is word for word what every SDK says. The error is an *Error with Status NotFound for a missing argument and TypeMismatch for one of
// another kind; a nil passed where an int is wanted is of another kind. A double is not narrowed.
//
// The generated exports call these. An export in the raw form can too:
//
//	var a [2]udf.Value
//	n := udf.Args(args, argc, a[:])
//	by, err := udf.ArgInt(a[:], n, 0, "increment", "by")
//	if err != nil {
//		return udf.FailErr(err) // "increment: argument 1 (by) is missing"
//	}
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

// ArgInt32 is argument i as a 32-bit integer; an error when it is missing, not an int, or does not fit
// 32 bits, which is reported as "<fn>: argument <i+1> (<p>) is out of range for a 32-bit int", with
// Status TypeMismatch. The parameters are ArgInt's.
func ArgInt32(a []Value, n int32, i int, fn, p string) (int32, error) {
	x, err := ArgInt(a, n, i, fn, p)
	if err != nil {
		return 0, err
	}
	if int64(int32(x)) != x {
		return 0, errorf(TypeMismatch, fn, ": argument ", strconv.Itoa(i+1), " (", p, ") is out of range for a 32-bit int")
	}
	return int32(x), nil
}

// ArgDouble is argument i as a double; an error when it is missing or of any other kind. An integer is
// not widened, as no reader widens one: a client that means 3.0 sends 3.0. The parameters are ArgInt's.
func ArgDouble(a []Value, n int32, i int, fn, p string) (float64, error) {
	v, err := argAt(a, n, i, fn, p)
	if err != nil {
		return 0, err
	}
	if v.Kind == KindDouble {
		return v.double(), nil
	}
	return 0, mismatch(fn, i, p, "a double")
}

// ArgBool is argument i as a boolean; an error when it is missing or not a bool. An integer is not
// read as one. The parameters are ArgInt's.
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

// ArgString is argument i as a string, copied into a new Go string; an error when it is missing or
// not a string. A blob is not read as one. The parameters are ArgInt's.
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

// ArgBlob is the bytes of argument i, which may be a string or a blob, copied into a new slice the
// caller owns; an error when it is missing or of any other kind, which reads "... is not a blob" (a
// string is taken all the same). The parameters are ArgInt's.
func ArgBlob(a []Value, n int32, i int, fn, p string) ([]byte, error) {
	v, err := argAt(a, n, i, fn, p)
	if err != nil {
		return nil, err
	}
	if v.Kind != KindStr && v.Kind != KindBlob {
		return nil, mismatch(fn, i, p, "a blob")
	}
	return v.Blob()
}

// ArgValue is argument i as it came, whatever its kind, a nil included; an error only when it is
// missing. The Value is good for the call. The parameters are ArgInt's.
func ArgValue(a []Value, n int32, i int, fn, p string) (Value, error) {
	return argAt(a, n, i, fn, p)
}

// ArgRest is every argument from index i on, as a new slice, for a function whose last parameter
// takes the rest; nil when there is none. a and n are the slice Args filled and the count it returned,
// so it holds no more arguments than a had room for: check the call with Arity(fn, len(a), argc)
// first, as the generated export does with a window of MaxArgs, and none is left out.
func ArgRest(a []Value, n int32, i int) []Value {
	if i >= int(n) {
		return nil
	}
	out := make([]Value, int(n)-i)
	copy(out, a[i:n])
	return out
}

// The optional forms: nil when the argument is absent or nil.

// OptInt is argument i as an integer when it was passed: nil, with no error, when the argument is
// absent or nil, and a pointer to the value otherwise. An argument of another kind is an error, as
// with ArgInt, whose parameters these are.
//
// The Opt functions all answer this way, and are what a pointer parameter of a `//udf:function` is
// decoded with:
//
//	k, err := udf.OptInt(a[:], n, 3, "topk", "k")
//	if err != nil {
//		return udf.FailErr(err)
//	}
//	limit := int64(10)
//	if k != nil {
//		limit = *k
//	}
func OptInt(a []Value, n int32, i int, fn, p string) (*int64, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	x, err := ArgInt(a, n, i, fn, p)
	return &x, err
}

// OptDouble is argument i as a double (an integer is not widened); nil when it is absent or nil, an error
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

// OptString is argument i as a string, copied out; nil when it is absent or nil, an error when it is
// of another kind.
func OptString(a []Value, n int32, i int, fn, p string) (*string, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	x, err := ArgString(a, n, i, fn, p)
	return &x, err
}

// OptValue is argument i as a value of any kind, nil when it is absent or nil. The pointer is into a,
// not a copy, so it is good while a is, and the error is always nil.
func OptValue(a []Value, n int32, i int, fn, p string) (*Value, error) {
	if argNone(a, n, i) {
		return nil, nil
	}
	return &a[i], nil
}
