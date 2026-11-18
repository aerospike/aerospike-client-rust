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
 * Exports a static method as an event handler: the Java face of the Rust SDK's {@code event!}. The server
 * has one event kind, {@code xdr-merge}, so there is one annotation, {@link XdrMerge}. The processor
 * generates the export that decodes the event into the method's parameters and the
 * {@code __udf_sig_<name>} export reading {@code event xdr-merge/1 ...}, which {@code event-set} checks
 * before binding. The parameter types are part of the kind, so a handler with another signature does not
 * compile. A handler returns a {@link Resolution}; an exception fails the call with its message.
 *
 * <pre>
 * {@literal @}On.XdrMerge("merge_counter")
 * static Resolution mergeCounter(EventRecord current, EventRecord incoming) {
 *     ...
 *     return Resolution.record(out);
 * }
 * </pre>
 *
 * <p>A handler may also take the raw form {@code int name(int args, int argc)}, exported as it is with only
 * the signature generated; it reads the two records with {@link Pair#decode} and answers by returning a
 * resolution's {@link Resolution#deliver}. The name is the method's when the annotation's value is empty.
 */
public final class On {
    private On() {
    }

    /**
     * {@code (EventRecord current, EventRecord incoming)}: the record as it is on this cluster and the one
     * that arrived over XDR; answers keep-local, take-remote, policy, delete or record.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface XdrMerge {
        String value() default "";
    }
}
