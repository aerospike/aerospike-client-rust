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
 * A {@link Function} or {@link Stream} parameter that may be absent or nil: the parameter is then
 * {@code null}, and the signature reads {@code name?: type}. A scalar parameter marked this way is boxed:
 * {@code @Nullable Long n}, not {@code long}.
 *
 * <pre>
 * {@literal @}Function("greet")
 * static String greet(String name, {@literal @}Nullable String title, {@literal @}Nullable Long times) {
 *     int n = times == null ? 1 : times.intValue();
 *     ...
 * }
 * // fn greet(name: str, title?: str, times?: int) -&gt; value
 * </pre>
 *
 * <p>Without it, a nil or missing argument fails the call before the method runs. The one exception is a
 * {@link Value} parameter, which takes a nil argument as a value whose {@link Value#isNil} is true and fails
 * only for a missing one; marked, it is {@code null} for both.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.SOURCE)
public @interface Nullable {
}
