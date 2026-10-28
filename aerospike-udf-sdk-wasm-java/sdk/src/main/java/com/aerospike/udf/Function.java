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
 * Exports a static method as a record UDF: the Java face of the Rust SDK's {@code export!}. The annotation
 * processor ({@code aerospike-udf-sdk-wasm-java-processor}) generates, in a class {@code <Class>Exports}
 * beside the annotated one, the exported {@code int name(int args, int argc)} that decodes the arguments
 * into the method's parameters and delivers its return value as the result, and the {@code __udf_sig_<name>}
 * export whose text it derives from the parameter types.
 *
 * <pre>
 * {@literal @}Function("increment")
 * static long increment(String site, long n) { ... }
 * // exports increment and __udf_sig_increment = "fn increment(site: str, n: int) -> value"
 * </pre>
 *
 * <p>Parameters: {@code long}, {@code int} ({@code int} in the signature), {@code double}, {@code boolean}
 * ({@code bool}), {@code String} ({@code str}), {@code byte[]} ({@code bytes}: a string or a blob),
 * {@link Value} ({@code any}), and {@code Value[]} as the last parameter for every remaining argument
 * ({@code any...}). A parameter marked {@link Nullable} may be absent or nil and is then {@code null}; a
 * scalar one is boxed ({@code Long}, {@code Integer}, {@code Double}, {@code Boolean}). A call with a
 * missing or mismatched argument, or more arguments than parameters, fails with a message naming the
 * argument, before the method runs.
 *
 * <p>Return: {@code void} (a nil result), {@code long} or {@code int}, {@code double}, {@code boolean},
 * {@code String}, {@code byte[]} (a blob), or {@link Value} (a scalar by value, guest bytes, a stored value
 * by position, or a built collection by handle). The method fails the call by throwing {@link UdfException}
 * with the message; any other exception fails it with the exception's message.
 *
 * <p>A method declared {@code (int args, int argc)} returning {@code int} is exported as it is: it takes
 * the raw handles and returns the status, exactly as a hand-written export does, and {@link #params} gives
 * the signature's parameter text, since nothing can be derived. This is the form for a hot path that
 * decodes its arguments its own way.
 *
 * <p>The generated class carries the module's {@code main}: name it as the TeaVM {@code mainClass}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface Function {
    /** The export name; the method's name when empty. */
    String value() default "";

    /**
     * The parameter text of the signature, {@code (name: type, ...)}, for a method that takes the raw
     * handles; derived from the parameters otherwise.
     */
    String params() default "";
}
