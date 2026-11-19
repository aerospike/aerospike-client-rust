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
 *
 * <pre>
 * private static final Value[] ARGS = new Value[2];
 *
 * {@literal @}Export(name = "add")
 * public static int add(int args, int argc) {
 *     try {
 *         int n = Params.args(args, argc, ARGS);
 *         Params.arity("add", 2, argc);        // more than it declares fails the call
 *
 *         long by = Params.argInt(ARGS, n, 0, "add", "by");
 *         Long cap = Params.optInt(ARGS, n, 1, "add", "cap");
 *         ...
 *         Udf.resultInt(total);
 *         return 0;
 *     }
 *     catch (Throwable t) {
 *         return Params.uncaught("add", t);
 *     }
 * }
 * </pre>
 */
public final class Params {
    private Params() {
    }

    /**
     * The most arguments a call can pass: 128, the bound of the import that decodes them. A function that
     * takes the rest ({@code Value[]} last) takes this many in all, and a call with more fails; none is ever
     * dropped.
     */
    public static final int MAX_ARGS = Slots.BATCH;

    /**
     * Fails a call that passed more arguments than the function declares: throws {@link UdfException} reading
     * {@code <fn> takes at most <declared> arguments, got <got>} when {@code got}, the export's {@code argc},
     * is larger. Too few is not its business: a missing argument is reported by the reader that wanted it. A
     * function that takes the rest ({@link #argRest}) calls it with {@link #MAX_ARGS}, the most a call can
     * pass, so that a longer call fails, naming the limit and the count, and no argument is dropped.
     */
    public static void arity(String fn, int declared, int got) {
        if (got > declared) {
            throw new UdfException(LowLevel.ERR, fn + " takes at most " + declared + " arguments, got " + got);
        }
    }

    private static Value at(Value[] a, int n, int i, String fn, String p) {
        if (i >= n || a[i].kind() == Value.ABSENT) {
            throw new UdfException(LowLevel.NOT_FOUND, fn + ": argument " + (i + 1) + " (" + p + ") is missing");
        }

        return a[i];
    }

    private static boolean none(Value[] a, int n, int i) {
        return i >= n || a[i].kind() == Value.ABSENT || a[i].kind() == Value.NIL;
    }

    private static UdfException mismatch(String fn, int i, String p, String want) {
        return new UdfException(LowLevel.TYPE_MISMATCH, fn + ": argument " + (i + 1) + " (" + p + ") is not " + want);
    }

    /** Copies the bytes of a string or blob argument, exactly as long as they are. */
    private static byte[] copy(Value v, String fn, int i, String p) {
        byte[] b = new byte[v.length()];

        if (b.length > 0) {
            int rc = LowLevel.bytes(v, b);

            if (rc < 0) {
                throw new UdfException(rc, fn + ": could not read argument " + (i + 1) + " (" + p + ")");
            }
        }

        return b;
    }

    //==========================================================
    // Required parameters.
    //

    /**
     * Argument {@code i}, counted from 0, of the {@code n} in {@code a}, as an integer. Throws
     * {@link UdfException} when it is missing ({@code <fn>: argument 2 (<p>) is missing}, with the status
     * {@link LowLevel#NOT_FOUND}) or is nil or of another kind ({@code ... is not an int}, with
     * {@link LowLevel#TYPE_MISMATCH}); {@code fn} and {@code p} are the function and parameter names
     * the message gives, and {@code n} is what {@link #args} returned. Every reader here takes the same five.
     */
    public static long argInt(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.INT) {
            throw mismatch(fn, i, p, "an int");
        }

        return v.payload();
    }

    /**
     * An integer argument that fits in 32 bits, for an {@code int} parameter; one outside that range is a
     * mismatch and throws {@link UdfException}, not a truncation.
     */
    public static int argInt32(Value[] a, int n, int i, String fn, String p) {
        long x = argInt(a, n, i, fn, p);

        if (x != (int) x) {
            throw mismatch(fn, i, p, "a 32-bit int");
        }

        return (int) x;
    }

    /**
     * A double argument. Throws {@link UdfException} when it is missing or of any other kind. An integer is
     * not widened, as no reader widens one: a client that means 3.0 sends 3.0.
     */
    public static double argDouble(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() == Value.DOUBLE) {
            return v.doubleBits();
        }

        throw mismatch(fn, i, p, "a double");
    }

    /**
     * A boolean argument. Throws {@link UdfException} when it is missing or of another kind; an integer 0 or
     * 1 is not one.
     */
    public static boolean argBool(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.BOOL) {
            throw mismatch(fn, i, p, "a bool");
        }

        return v.payload() != 0;
    }

    /**
     * The text of a string argument, of any length, decoded from UTF-8 into a new {@code String}. Bytes that
     * are not UTF-8 are not refused: each malformed sequence is replaced with U+FFFD, so the text is no
     * longer what the client sent. {@link #argBlob}, a {@code byte[]} parameter, is the way to take raw
     * bytes: it accepts a string argument too and hands its bytes over untouched. Throws
     * {@link UdfException} when the argument is missing or is not a string.
     */
    public static String argString(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.STR) {
            throw mismatch(fn, i, p, "a string");
        }

        byte[] b = copy(v, fn, i, p);

        return LowLevel.string(b, b.length);
    }

    /**
     * The bytes of a string or a blob argument, copied into a new array exactly as long as they are, with
     * nothing decoded or replaced: the reader for raw bytes, whichever of the two the client sent. Throws
     * {@link UdfException} when it is missing or of another kind.
     */
    public static byte[] argBlob(Value[] a, int n, int i, String fn, String p) {
        Value v = at(a, n, i, fn, p);

        if (v.kind() != Value.STR && v.kind() != Value.BLOB) {
            throw mismatch(fn, i, p, "a string or a blob");
        }

        return copy(v, fn, i, p);
    }

    /**
     * The argument as it is, of any kind: nil is a value whose {@link Value#isNil} is true. Throws
     * {@link UdfException} only when it is missing. The value is the one in {@code a}, which the next call's
     * {@link #args} overwrites: it is good for this call.
     */
    public static Value argValue(Value[] a, int n, int i, String fn, String p) {
        return at(a, n, i, fn, p);
    }

    /**
     * Every argument from {@code i} on, in a new array: empty when there are no more. For a last parameter
     * that takes the rest; nothing is checked, so nil arguments are in it as nil values.
     */
    public static Value[] argRest(Value[] a, int n, int i) {
        int k = Math.max(0, n - i);
        Value[] out = new Value[k];

        System.arraycopy(a, i, out, 0, k);

        return out;
    }

    //==========================================================
    // Nullable parameters: null when the argument is absent or nil.
    //

    /**
     * As {@link #argInt}, or null when the argument is absent or nil; one of another kind still throws
     * {@link UdfException}. For a parameter marked {@link Nullable}.
     */
    public static Long optInt(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : argInt(a, n, i, fn, p);
    }

    /**
     * As {@link #argInt32}, or null when the argument is absent or nil; one of another kind still throws
     * {@link UdfException}. For a parameter marked {@link Nullable}.
     */
    public static Integer optInt32(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : argInt32(a, n, i, fn, p);
    }

    /**
     * As {@link #argDouble}, or null when the argument is absent or nil; one of another kind still throws
     * {@link UdfException}. For a parameter marked {@link Nullable}.
     */
    public static Double optDouble(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : argDouble(a, n, i, fn, p);
    }

    /**
     * As {@link #argBool}, or null when the argument is absent or nil; one of another kind still throws
     * {@link UdfException}. For a parameter marked {@link Nullable}.
     */
    public static Boolean optBool(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : argBool(a, n, i, fn, p);
    }

    /**
     * As {@link #argString}, or null when the argument is absent or nil; one of another kind still throws
     * {@link UdfException}. For a parameter marked {@link Nullable}.
     */
    public static String optString(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : argString(a, n, i, fn, p);
    }

    /**
     * As {@link #argBlob}, or null when the argument is absent or nil; one of another kind still throws
     * {@link UdfException}. For a parameter marked {@link Nullable}.
     */
    public static byte[] optBlob(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : argBlob(a, n, i, fn, p);
    }

    /**
     * The argument as it is, or null when it is absent or nil. It never throws; {@code fn} and {@code p} are
     * taken only to match the other readers.
     */
    public static Value optValue(Value[] a, int n, int i, String fn, String p) {
        return none(a, n, i) ? null : a[i];
    }

    //==========================================================
    // Results.
    //

    /**
     * Delivers a {@link Value} as the result: a scalar by value, guest bytes by address, a built collection
     * by handle, a stored value by position; null or absent is nil. It is {@link Udf#result}.
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

    /** Delivers a string as the result; null is nil, which is the difference from {@link Udf#resultString}. */
    public static void resultString(String s) {
        if (s == null) {
            Udf.resultNil();
        }
        else {
            Udf.resultString(s);
        }
    }

    /**
     * Delivers a byte array as a blob result; null is nil, which is the difference from
     * {@link Udf#resultBlob}.
     */
    public static void resultBlob(byte[] b) {
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

    /**
     * The record the call is about, for a method that takes one as its first parameter. It is the same object
     * on every call, standing for whichever record the call in hand is about; in a stream that is the record
     * the query thread is on, which cannot be written.
     */
    public static Record record() {
        return Record.current();
    }

    /**
     * Decodes the call's arguments into {@code out}, in one import, filling (or allocating, where an element
     * is null) a Value per slot: {@code handle} and {@code argc} are the export's two parameters. Returns how
     * many were filled, bounded by {@code out.length} and {@link #MAX_ARGS}, which is the {@code n} the
     * readers here take; 0 when there are none. It does not fail for a call that passed more than
     * {@code out} holds: the surplus shows only as an {@code argc} larger than the count returned, and
     * {@link #arity} is the check that turns it into a failure. Keep {@code out} in a static field so that a
     * call allocates no array.
     */
    public static int args(int handle, int argc, Value[] out) {
        return LowLevel.args(handle, argc, out);
    }

    /**
     * The text behind a {@code __udf_sig_<name>} export, such as {@code fn add(by: int, cap?: int) -> value},
     * as a NUL-terminated byte array to keep in a static field. The export returns {@link #signature} of it.
     * The server serves the text to clients, which can check a call against it; it must say what the export
     * really takes.
     */
    public static byte[] signatureBytes(String text) {
        byte[] t = LowLevel.bytes(text);
        byte[] out = new byte[t.length + 1];
        System.arraycopy(t, 0, out, 0, t.length);
        return out;
    }

    /**
     * The address the host reads a signature from, to return from a {@code __udf_sig_<name>} export: valid
     * until the next allocation, which is after the host read it. {@code text} is what
     * {@link #signatureBytes} made.
     */
    public static int signature(byte[] text) {
        return Address.ofData(text).toInt();
    }

    /**
     * Fails the call for an exception the method did not catch, with the message
     * {@code <fn>: <the exception's message>}, or the exception's class name when it has no message. Returns
     * the non-zero status for the export to return.
     */
    public static int uncaught(String fn, Throwable t) {
        String m = t.getMessage();

        return Udf.fail(fn + ": " + (m == null || m.isEmpty() ? "uncaught " + t.getClass().getName() : m));
    }
}
