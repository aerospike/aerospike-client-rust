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
 * Exports a static method as an event handler, one annotation per kind: the Java face of the Rust SDK's
 * {@code event!}. The processor generates the export that decodes the event into the method's parameters
 * and the {@code __udf_sig_<name>} export reading {@code event <kind>/1 ...}, which {@code event-set}
 * checks before binding. The parameter types are part of the kind, so a handler with another signature
 * does not compile. A handler answers with the {@link Events} or {@link Conflict} methods and returns
 * what they return (the status); an exception fails the call with its message.
 *
 * <pre>
 * {@literal @}On.BeforeWrite("guard_account")
 * static int guardAccount(EventRecord before, EventRecord after, Events.WriteInfo write) {
 *     ...
 *     return Events.accept();
 * }
 *
 * {@literal @}On.XdrMerge("merge_counter")
 * static int mergeCounter(EventRecord current, EventRecord incoming, Conflict conflict) {
 *     ...
 *     return Conflict.record(out, -1);
 * }
 * </pre>
 *
 * <p>Every kind also takes the raw form {@code int name(int args, int argc)}, exported as it is with only
 * the signature generated. The name is the method's when the annotation's value is empty.
 */
public final class On {
    private On() {
    }

    /** {@code (EventRecord before, EventRecord after, Events.WriteInfo write)}; answers accept, reject or record. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface BeforeWrite {
        String value() default "";
    }

    /** {@code (EventRecord before, EventRecord after, Events.WriteInfo write)}; answers done. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface AfterWrite {
        String value() default "";
    }

    /** {@code (EventRecord record, Events.DeleteInfo delete)}; answers allow or refuse. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface BeforeDelete {
        String value() default "";
    }

    /** {@code (EventRecord record, Events.DeleteInfo delete)}; answers done. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface AfterDelete {
        String value() default "";
    }

    /** {@code (EventRecord record, Events.ExpireInfo expire)}; answers expire or keep. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface BeforeExpire {
        String value() default "";
    }

    /** {@code (EventRecord record, Events.ExpireInfo evict)}; answers evict or protect. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface BeforeEvict {
        String value() default "";
    }

    /** {@code (EventRecord record, Events.ReadInfo read)}; answers unchanged or record. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface AfterRead {
        String value() default "";
    }

    /** {@code (Events.TruncateInfo truncate)}; answers allow or refuse. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface BeforeTruncate {
        String value() default "";
    }

    /** {@code (Events.TruncateInfo truncate)}; answers done. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface AfterTruncate {
        String value() default "";
    }

    /** {@code (EventRecord record, Events.ShipInfo ship)}; answers ship, skip or record. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface BeforeShip {
        String value() default "";
    }

    /** {@code (Events.TransferInfo transfer)}; answers done. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface PartitionTransfer {
        String value() default "";
    }

    /** {@code (Events.ClusterInfo cluster)}; answers done. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface ClusterChange {
        String value() default "";
    }

    /** {@code (Events.LifecycleInfo lifecycle)}; answers done. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface NodeLifecycle {
        String value() default "";
    }

    /** {@code (Events.TimerInfo timer)}; runs as a record UDF and answers done. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface Timer {
        String value() default "";
    }

    /**
     * {@code (EventRecord current, EventRecord incoming, Conflict conflict)}; answers keep-local, take-remote,
     * policy or record. The two records are {@code conflict.local} and {@code conflict.remote}.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.SOURCE)
    public @interface XdrMerge {
        String value() default "";
    }
}
