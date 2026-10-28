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

import org.teavm.interop.Address;

/**
 * A list built on the host: the Java face of the Rust SDK's {@code List}. Every {@code add} is one import
 * and throws {@link UdfException} when the host refuses it; they chain. See {@link UdfMap}.
 */
public final class UdfList {
    private final int handle;

    private UdfList(int handle) {
        this.handle = handle;
    }

    public static UdfList create(int capacity) {
        int h = Natives.listNew(capacity);

        if (h <= 0) {
            throw new UdfException("could not build a list");
        }

        return new UdfList(h);
    }

    public static UdfList create() {
        return create(8);
    }

    /** A list over a handle the low-level calls built. */
    public static UdfList wrap(int handle) {
        return new UdfList(handle);
    }

    public int handle() {
        return handle;
    }

    public int size() {
        return Natives.collSize(handle);
    }

    public Value asValue() {
        return Value.ofHandle(handle);
    }

    private UdfList check(int rc) {
        if (rc != Udf.OK) {
            throw new UdfException("could not add to the list (" + rc + ")");
        }

        return this;
    }

    public UdfList add(long value) {
        return check(Natives.listPushInt(handle, value));
    }

    public UdfList add(double value) {
        return check(Natives.listPush(handle, Natives.valDouble(value)));
    }

    public UdfList add(boolean value) {
        return check(Natives.listPush(handle, Natives.valBool(value ? 1 : 0)));
    }

    public UdfList add(String value) {
        byte[] v = Udf.bytes(value);
        return check(Natives.listPushStr(handle, Address.ofData(v), v.length));
    }

    /** A blob. */
    public UdfList add(byte[] value) {
        return check(Natives.listPush(handle, Natives.valBlob(Address.ofData(value), value.length)));
    }

    /** A value of any kind: a scalar, guest bytes, a stored value by position, a built collection. */
    public UdfList add(Value value) {
        switch (value.kind) {
        case Value.INT:
            return add(value.payload);
        case Value.DOUBLE:
            return add(value.asDouble());
        case Value.BOOL:
            return add(value.payload != 0);
        case Value.NIL:
            return addNull();
        case Value.GUEST_STR:
            return check(Natives.listPushStr(handle, Address.ofData(value.bytes), value.bytesLen));
        case Value.GUEST_BLOB:
            return check(Natives.listPush(handle, Natives.valBlob(Address.ofData(value.bytes), value.bytesLen)));
        case Value.GUEST_GEOJSON:
            return check(Natives.listPush(handle, Natives.valGeoJson(Address.ofData(value.bytes), value.bytesLen)));
        case Value.HANDLE:
            return check(Natives.listPush(handle, (int) value.payload));
        case Value.ABSENT:
            throw new UdfException("could not add an absent value to the list");
        default:
            return check(Natives.listPushAt(handle, value.src(), value.at()));
        }
    }

    public UdfList add(UdfMap value) {
        return check(Natives.listPush(handle, value.handle()));
    }

    public UdfList add(UdfList value) {
        return check(Natives.listPush(handle, value.handle));
    }

    public UdfList addNull() {
        return check(Natives.listPush(handle, Natives.valNil()));
    }

    public UdfList addGeoJson(String doc) {
        byte[] d = Udf.bytes(doc);
        return check(Natives.listPush(handle, Natives.valGeoJson(Address.ofData(d), d.length)));
    }

    /** The first {@code n} of {@code values} in one import. */
    public UdfList addAll(Value[] values, int n) {
        int added = Udf.listPushValues(handle, values, n);

        if (added != n) {
            throw new UdfException("could not add value " + added);
        }

        return this;
    }

    /** Every element of a stored list, by position. */
    public UdfList addAll(Value stored) {
        for (Value v : Udf.list(stored)) {
            add(v);
        }

        return this;
    }
}
