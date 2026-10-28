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

/** What a {@code before-delete} or {@code before-truncate} handler returns. */
public final class Gate extends Answer {
    private Gate(String verdict, long code, String message) {
        super(verdict, code, message, 0, -1);
    }

    public static Gate allow() {
        return new Gate("allow", 0, null);
    }

    public static Gate refuse(long code, String message) {
        return new Gate("refuse", code, message);
    }
}
