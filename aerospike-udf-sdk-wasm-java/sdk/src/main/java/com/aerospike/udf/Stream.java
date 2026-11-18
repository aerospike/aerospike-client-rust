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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Exports a class as a stream (aggregation) UDF: the Java face of the Rust SDK's {@code stream!}. The
 * class has a {@code record} method, called with every record a query thread reads, and an {@code end}
 * method, called once afterwards, whose return is the partial; its fields are the stream's state, since
 * one instance sees the whole stream. The processor instantiates the class once (it needs a no-argument
 * constructor the generated class can reach) and exports {@code <name>_record} and {@code <name>_end}
 * with the signature {@code stream <name>} followed by the two parameter lists.
 *
 * <pre>
 * {@literal @}Stream("sum")
 * final class Sum {
 *     private long total;
 *
 *     void record(Record rec, String bin) { total += rec.getLong(bin, 0); }
 *
 *     long end(String bin) { return total; }
 * }
 * </pre>
 *
 * <p>Parameters and returns are as for {@link Function}: {@code record} returns {@code void}, {@code end}
 * the partial. {@code record} may take the {@link Record} as its first parameter, which is the record the
 * query thread is on and is read-only: a write to it throws {@link UdfException}. {@code end} takes none,
 * since it runs after the last record. In the raw form both are {@code int name(int args, int argc)} and {@link #params} and
 * {@link #endParams} give the signature's parameter text, which is the form for a stream that reads its
 * arguments once, on the first record, and keeps them.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Stream {
    /** The stream's name, which a query names. */
    String value();

    /** The parameter text of {@code record} in the signature, for the raw form. */
    String params() default "";

    /** The parameter text of {@code end} in the signature, for the raw form. */
    String endParams() default "";
}
