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

package com.aerospike.udf;

import java.util.List;
import java.util.Map;

import org.teavm.interop.Address;

/**
 * What the generated exports call: an argument decoded into a parameter of the method's type, failing
 * the call with a message that names the argument when it is missing or of another kind; a return value
 * delivered as the result; an uncaught exception turned into a failure. The arguments come from
 * {@link #args} as a {@link Value} array and a count. Nothing here is specific to a generated class,
 * so a hand-written export may use it too.
 */
public final class Params {
    private Params() {
    }

    /** Fails a call that passed more arguments than the function declares, unless it takes the rest. */
    public static void arity(String fn, int declared, int got) {
        if (got > declared) {
            throw new UdfException(fn + " takes at most " + declared + " arguments, got " + got);
        }
    }

    private static Value at(Value[] a, int n, int i, String fn, String p) {
        if (i >= n || a[i].kind() == Value.ABSENT) {
            throw new UdfException(fn + ": argument " + (i + 1) + " (" + p + ") is missing");
        }

        return a[i];
    }

    private static boolean none(Value[] a, int n, int i) {
        return i >= n || a[i].kind() == Value.ABSENT || a[i].kind() == Value.NIL;
    }

    private static UdfException mismatch(String fn, int i, String p, String want) {
        return new UdfException(fn + ": argument " + (i + 1) + " (" + p + ") is not " + want);
    }

    /** Copies the bytes of a string or blob argument, exactly as long as they are. */
    private static byte[] copy(Value v, String fn, int i, String p) {
        byte[] b = new byte[v.length()];

        if (b.length > 0 && LowLevel.bytes(v, b) < 0) {
            throw new UdfException(fn + ": could not read argument " + (i + 1) + " (" + p + ")");
        }

        return b;
    }

    //==========================================================
    // Required parameters.
    //

    /**
     * Argument {@code i}, counted from 0, of the {@code n} in {@code a}, as an integer. Throws
     * {@link UdfException} when it is missing or of another kind; {@code fn} and {@code p} are the function
     * and parameter names its message gives. Every reader here takes the same five.
     */
    public static long int64(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.INT) {
            throw mismatch(fn, i, p, "an int");
        }

        return v.payload();
    }

    /** An integer argument that fits in 32 bits; one that does not is a mismatch. */
    public static int int32(Value[] a, int n, int i, String fn, String p) {
        long x = int64(a, n, i, fn, p);

        if (x != (int) x) {
            throw mismatch(fn, i, p, "a 32-bit int");
        }

        return (int) x;
    }

    /** A double, or an int widened to one. */
    public static double float64(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        if (v.kind() == Value.INT) {
            return (double) v.payload();
        }

        throw mismatch(fn, i, p, "a double");
    }

    /** A boolean argument. */
    public static boolean bool(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.BOOL) {
            throw mismatch(fn, i, p, "a bool");
        }

        return v.payload() != 0;
    }

    /** The text of a string argument, copied out. */
    public static String str(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.STR) {
            throw mismatch(fn, i, p, "a string");
        }

        byte[] b = copy(v, fn, i, p);

        return LowLevel.string(b, b.length);
    }

    /** The bytes of a string or a blob. */
    public static byte[] bytes(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.STR && v.kind() != Value.BLOB) {
            throw mismatch(fn, i, p, "a string or a blob");
        }

        return copy(v, fn, i, p);
    }

    /** Any value, nil included. */
    public static Value any(Value[] a, int n, int i, String fn, String p) {
        return at(a, n, i, fn, p);
    }

    /** Every argument from {@code i} on. */
    public static Value[] rest(Value[] a, int n, int i) {
        int k = Math.max(0, n - i);
        Value[] out = new Value[k];

        System.arraycopy(a, i, out, 0, k);

        return out;
    }

    //==========================================================
    // Nullable parameters: null when the argument is absent or nil.
    //

    /** As {@link #int64}, or null when the argument is absent or nil. */
    public static Long optInt64(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : int64(a, n, i, fn, p);
    }

    /** As {@link #int32}, or null when the argument is absent or nil. */
    public static Integer optInt32(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : int32(a, n, i, fn, p);
    }

    /** As {@link #float64}, or null when the argument is absent or nil. */
    public static Double optFloat64(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : float64(a, n, i, fn, p);
    }

    /** As {@link #bool}, or null when the argument is absent or nil. */
    public static Boolean optBool(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : bool(a, n, i, fn, p);
    }

    /** As {@link #str}, or null when the argument is absent or nil. */
    public static String optStr(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : str(a, n, i, fn, p);
    }

    /** As {@link #bytes}, or null when the argument is absent or nil. */
    public static byte[] optBytes(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : bytes(a, n, i, fn, p);
    }

    /** The argument as it is, or null when it is absent or nil. */
    public static Value optAny(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : a[i];
    }

    //==========================================================
    // Results.
    //

    /**
     * Delivers a {@link Value} as the result: a scalar by value, guest bytes by address, a built collection
     * by handle, a stored value by position; null or absent is nil.
     */
    public static void result(Value v) {
        Udf.result(v);
    }

    /** Delivers a built map as the result; null is nil. */
    public static void result(MapBuilder m) {
        Udf.result(m == null ? null : m.asValue());
    }

    /** Delivers a built list as the result; null is nil. */
    public static void result(ListBuilder l) {
        Udf.result(l == null ? null : l.asValue());
    }

    /** Delivers a stored map as the result, by position; null is nil. */
    public static void result(MapValue m) {
        Udf.result(m == null ? null : m.asValue());
    }

    /** Delivers a stored list as the result, by position; null is nil. */
    public static void result(ListValue l) {
        Udf.result(l == null ? null : l.asValue());
    }

    /** Delivers a {@code java.util.List} as a list result, its elements as a builder takes them; null is nil. */
    public static void result(List<?> l) {
        Udf.result(l == null ? null : Udf.list(l.size()).addAll(l).asValue());
    }

    /** Delivers a {@code java.util.Map} as a map result, its entries as a builder takes them; null is nil. */
    public static void result(Map<?, ?> m) {
        Udf.result(m == null ? null : Udf.map(m.size()).putAll(m).asValue());
    }

    /** Delivers a string as the result; null is nil. */
    public static void resultStr(String s) {
        if (s == null) {
            Udf.resultNil();
        }
        else {
            Udf.resultStr(s);
        }
    }

    /** Delivers a byte array as a blob result; null is nil. */
    public static void resultBytes(byte[] b) {
        if (b == null) {
            Udf.resultNil();
        }
        else {
            Udf.resultBlob(b);
        }
    }

    //==========================================================
    // What every export starts from: the record, the arguments, the signature.
    //

    /** The record the call is about, for a method that takes one as its first parameter. */
    public static Record record() {
        return Record.current();
    }

    /**
     * Decodes the call's arguments into {@code out}, in one import, filling (or allocating) a Value per slot.
     * Returns how many there are, bounded by {@code out.length}.
     */
    public static int args(int handle, int argc, Value[] out) {
        return LowLevel.args(handle, argc, out);
    }

    /**
     * The text behind a {@code __udf_sig_<name>} export: a NUL-terminated byte array, kept in a static field.
     * The export returns {@link #signature} of it.
     */
    public static byte[] signatureBytes(String text) {
        byte[] t = LowLevel.bytes(text);
        byte[] out = new byte[t.length + 1];
        System.arraycopy(t, 0, out, 0, t.length);
        return out;
    }

    /** The address the host reads a signature from: valid until the next allocation, which is after the host read it. */
    public static int signature(byte[] text) {
        return Address.ofData(text).toInt();
    }

    /** Fails the call for an exception the method did not catch, with its message. Returns the status. */
    public static int uncaught(String fn, Throwable t) {
        String m = t.getMessage();

        return Udf.fail(fn + ": " + (m == null || m.isEmpty() ? "uncaught " + t.getClass().getName() : m));
    }
}
