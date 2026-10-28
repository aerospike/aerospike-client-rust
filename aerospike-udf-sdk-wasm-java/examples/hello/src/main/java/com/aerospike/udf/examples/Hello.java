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

package com.aerospike.udf.examples;

import org.teavm.interop.Address;
import org.teavm.interop.Export;
import org.teavm.interop.Import;

/**
 * The bare contract, without the SDK: a UDF that answers a constant and logs a line, one that adds its
 * two arguments read as 16-byte values by position, and probes of what a Java exception does to a call.
 * {@code tests/src/udf_java.rs} runs it. Note the one rule of this backend: {@code Address.ofData(buf)}
 * is taken inside the expression that hands it to the host, never kept across an allocation.
 */
public final class Hello {
    @Import(module = "aerospike", name = "result_int")
    private static native void resultInt(long value);

    @Import(module = "aerospike", name = "log")
    private static native void log(int level, Address message, int length);

    @Import(module = "aerospike", name = "args_values")
    private static native int argsValues(int args, Address out, int capacity);

    private static final byte[] SCRATCH = new byte[2048];

    @Export(name = "hello")
    public static int hello(int args, int argc) {
        byte[] msg = "hello from java".getBytes();
        System.arraycopy(msg, 0, SCRATCH, 0, msg.length);
        log(1, Address.ofData(SCRATCH), msg.length);
        resultInt(42);
        return 0;
    }

    @Export(name = "add")
    public static int add(int args, int argc) {
        Address out = Address.ofData(SCRATCH);
        int n = argsValues(args, out, 2 * 16);
        long a = n > 0 ? out.add(8).getLong() : 0;
        long b = n > 1 ? out.add(24).getLong() : 0;
        resultInt(a + b);
        return 0;
    }

    /** What an uncaught Java exception does to a call: the call fails. */
    @Export(name = "probe_throw")
    public static int probeThrow(int args, int argc) {
        int[] a = new int[1];
        resultInt(a[argc + 5]);
        return 0;
    }

    /** A caught exception: Java's try/catch, inside the module. */
    @Export(name = "probe_catch")
    public static int probeCatch(int args, int argc) {
        try {
            int[] a = new int[1];
            resultInt(a[argc + 5]);
        } catch (ArrayIndexOutOfBoundsException e) {
            resultInt(7);
        }
        return 0;
    }

    public static void main(String[] args) {
    }
}
