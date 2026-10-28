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

/**
 * What the generated exports call: an argument decoded into a parameter of the method's type, failing
 * the call with a message that names the argument when it is missing or of another kind; a return value
 * delivered as the result; an uncaught exception turned into a failure. The arguments come from
 * {@link Udf#args} as a {@link Value} array and a count. Nothing here is specific to a generated class,
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
        if (i >= n || a[i].kind == Value.ABSENT) {
            throw new UdfException(fn + ": argument " + (i + 1) + " (" + p + ") is missing");
        }

        return a[i];
    }

    private static boolean none(Value[] a, int n, int i) {
        return i >= n || a[i].kind == Value.ABSENT || a[i].kind == Value.NIL;
    }

    private static UdfException mismatch(String fn, int i, String p, String want) {
        return new UdfException(fn + ": argument " + (i + 1) + " (" + p + ") is not " + want);
    }

    /** Copies the bytes of a string or blob argument, exactly as long as they are. */
    private static byte[] copy(Value v, String fn, int i, String p) {
        byte[] b = new byte[v.len];

        if (v.len > 0 && Udf.bytes(v, b) < 0) {
            throw new UdfException(fn + ": could not read argument " + (i + 1) + " (" + p + ")");
        }

        return b;
    }

    //==========================================================
    // Required parameters.
    //

    public static long int64(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind != Value.INT) {
            throw mismatch(fn, i, p, "an int");
        }

        return v.payload;
    }

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

        if (v.kind == Value.DOUBLE) {
            return v.asDouble();
        }

        if (v.kind == Value.INT) {
            return (double) v.payload;
        }

        throw mismatch(fn, i, p, "a double");
    }

    public static boolean bool(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind != Value.BOOL) {
            throw mismatch(fn, i, p, "a bool");
        }

        return v.payload != 0;
    }

    public static String str(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind != Value.STR) {
            throw mismatch(fn, i, p, "a string");
        }

        byte[] b = copy(v, fn, i, p);

        return Udf.string(b, b.length);
    }

    /** The bytes of a string or a blob. */
    public static byte[] bytes(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind != Value.STR && v.kind != Value.BLOB) {
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

    public static Long optInt64(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : int64(a, n, i, fn, p);
    }

    public static Integer optInt32(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : int32(a, n, i, fn, p);
    }

    public static Double optFloat64(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : float64(a, n, i, fn, p);
    }

    public static Boolean optBool(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : bool(a, n, i, fn, p);
    }

    public static String optStr(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : str(a, n, i, fn, p);
    }

    public static byte[] optBytes(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : bytes(a, n, i, fn, p);
    }

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
        if (v == null) {
            Udf.resultNil();
            return;
        }

        switch (v.kind) {
        case Value.INT:
            Udf.resultInt(v.payload);
            break;
        case Value.DOUBLE:
            Udf.resultDouble(v.asDouble());
            break;
        case Value.BOOL:
            Udf.resultBool(v.payload != 0);
            break;
        case Value.NIL:
        case Value.ABSENT:
            Udf.resultNil();
            break;
        case Value.GUEST_STR:
            Udf.resultStr(v.bytes, v.bytesLen);
            break;
        case Value.GUEST_BLOB:
            Udf.resultBlob(v.bytes, v.bytesLen);
            break;
        case Value.GUEST_GEOJSON:
            Udf.resultGeoJson(v.bytes, v.bytesLen);
            break;
        case Value.HANDLE:
            Udf.resultHandle((int) v.payload);
            break;
        default:
            Udf.resultValue(v);
            break;
        }
    }

    public static void result(UdfMap m) {
        if (m == null) {
            Udf.resultNil();
        }
        else {
            Udf.resultHandle(m.handle());
        }
    }

    public static void result(UdfList l) {
        if (l == null) {
            Udf.resultNil();
        }
        else {
            Udf.resultHandle(l.handle());
        }
    }

    public static void resultStr(String s) {
        if (s == null) {
            Udf.resultNil();
        }
        else {
            Udf.resultStr(s);
        }
    }

    public static void resultBytes(byte[] b) {
        if (b == null) {
            Udf.resultNil();
        }
        else {
            Udf.resultBlob(b);
        }
    }

    /** Fails the call for an exception the method did not catch, with its message. Returns the status. */
    public static int uncaught(String fn, Throwable t) {
        String m = t.getMessage();

        return Udf.fail(fn + ": " + (m == null || m.isEmpty() ? "uncaught " + t.getClass().getName() : m));
    }
}
