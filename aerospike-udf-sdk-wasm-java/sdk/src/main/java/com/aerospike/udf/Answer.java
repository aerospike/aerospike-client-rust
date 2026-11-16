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
 * What an event handler returns, delivered by the generated export: a verdict word with, where the kind
 * takes them, a code and message, a bins map and a TTL. The kinds' own types ({@link Decision},
 * {@link Gate}, {@link Expiry}, {@link Eviction}, {@link ReadResult}, {@link ShipDecision},
 * {@link Resolution}) are the ones a handler names; this is what they have in common.
 */
public abstract class Answer {
    final String verdict;
    final long code;
    final String message;
    final int bins;
    final long ttl;

    Answer(String verdict, long code, String message, int bins, long ttl) {
        this.verdict = verdict;
        this.code = code;
        this.message = message;
        this.bins = bins;
        this.ttl = ttl;
    }

    /** Sets the answer as the call's result. Called by the generated export; returns the status. */
    public int deliver() {
        return Events.answer(verdict, code, message, bins, ttl);
    }

    /** The verdict word as the server reads it, such as {@code accept} or {@code keep-local}. */
    public String verdict() {
        return verdict;
    }
}
